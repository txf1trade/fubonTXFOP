"""
富邦新一代 API 選擇權看盤網頁
- 登入有兩種方式：(1) 開啟網頁後在登入視窗輸入身分證字號 / 密碼 / 憑證密碼並上傳 .p12；
  (2) 在 .env 設定 FUBON_ID / FUBON_PWD / FUBON_CERT_PATH / FUBON_CERT_PWD，網頁（或伺服器啟動時）直接讀取登入
- 選擇權鏈跟著台指期的「日盤 / 夜盤」切換（網頁點台指期卡片切換，選擇權鏈同步換成該時段的報價與力道）
- 上方：加權指數 / 櫃買指數 / 台指近月 / 個股
- 中間：到期別頁籤（月選、週三、週五）
- 下方：選擇權鏈（履約價檔數可選「全部」或「中心上下各 N 檔」），表頭下方有全市場總量 / 力道加總
- 力道資料以 SQLite（flow.db）保存，可顯示「今日 / 本週」各履約價累積

速率限制（官方：期貨行情 Web API 300 次/分）：本程式內建節流，預設上限 240 次/分。
注意：行情登入狀態與力道累積都存在「單一行程」記憶體中，部署時請只開 1 個 worker（多執行緒即可）。
"""
import os
import re
import hmac
import base64
import json
import time
import atexit
import shutil
import sqlite3
import tempfile
import threading
import datetime as dt
from collections import deque
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_from_directory

from fubon_neo.sdk import FubonSDK

try:  # SDK 2.2.4 以後才有
    from fubon_neo.fugle_marketdata.rest.base_rest import FugleAPIError
except Exception:  # noqa
    class FugleAPIError(Exception):
        status_code = None
        response_text = None

load_dotenv()   # 只讀取「非機密」的顯示 / 節流設定；帳號密碼不再從 .env 讀取

STOCK_SYMBOL = os.getenv("STOCK_SYMBOL", "2330")
TWSE_FALLBACK = os.getenv("TWSE_INDEX_SYMBOL", "IX0001")
TPEX_FALLBACK = os.getenv("TPEX_INDEX_SYMBOL", "IX0043")
FUT_ALIAS = os.getenv("FUT_ALIAS", "TXF1!")  # 連續月別名：最近月臺股期貨
OPTION_WINDOW = int(os.getenv("OPTION_WINDOW", "10"))      # 預設中心上下各幾檔履約價（網頁設定可覆寫）
CHAIN_TTL = float(os.getenv("CHAIN_TTL", "12"))            # 選擇權鏈快取秒數
FAR_TTL = float(os.getenv("FAR_TTL", "120"))               # 「全部履約價」模式下，遠離價平的履約價 REST 更新間隔(秒)
FUT_MAX_PER_MIN = int(os.getenv("FUT_MAX_PER_MIN", "240"))  # 期權行情節流上限
FLOW_EXTRA = int(os.getenv("FLOW_EXTRA", "1"))              # 啟動預訂閱範圍：顯示範圍外再多訂幾檔
FLOW_PREFETCH = int(os.getenv("FLOW_PREFETCH", "2"))        # 登入後預先訂閱最近幾個到期別
BOOK_MAX = int(os.getenv("BOOK_MAX", "20"))                 # 同時訂閱五檔(books)的商品數上限
WS_MAX = int(os.getenv("WS_MAX", "180"))                    # trades 訂閱上限；與 BOOK_MAX 合計請低於你帳號的 WebSocket 訂閱上限
DB_PATH = os.getenv("DB_PATH", "")                          # SQLite 檔案路徑，預設 app.py 旁的 flow.db
DB_KEEP_DAYS = int(os.getenv("DB_KEEP_DAYS", "180"))        # 力道資料保留天數
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "5000"))
MAX_CERT_BYTES = 64 * 1024

# 從 .env 直接登入（不想每次輸入時使用）。這幾個是機密，.env 請只放在伺服器上並限制檔案權限。
ENV_ID = os.getenv("FUBON_ID", "").strip().upper()
ENV_PWD = os.getenv("FUBON_PWD", "")
ENV_CERT_PATH = os.getenv("FUBON_CERT_PATH", "").strip()     # 相對路徑以 app.py 所在資料夾為基準
ENV_CERT_PWD = os.getenv("FUBON_CERT_PWD", "")
ENV_CERT_B64 = os.getenv("FUBON_CERT_B64", "").strip()           # 部署在 Render 時：把 .p12 轉成 base64 放這裡，免上傳檔案
ENV_AUTO_LOGIN = os.getenv("ENV_AUTO_LOGIN", "0") == "1"      # 1 = 伺服器啟動時就自動用 .env 登入（不用開網頁）

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if not DB_PATH:
    DB_PATH = os.path.join(BASE_DIR, "flow.db")


def _db_dir_writable(p):
    d = os.path.dirname(os.path.abspath(p))
    try:
        os.makedirs(d, exist_ok=True)
        t = os.path.join(d, ".w_test")
        with open(t, "w") as fp:
            fp.write("1")
        os.remove(t)
        return True
    except Exception:  # noqa
        return False


if not _db_dir_writable(DB_PATH):
    # 例如 Render 免費方案沒有 Disk、/var/data 不能寫：改存程式資料夾，再不行就存系統暫存區（重啟 / 重新部署後會消失）
    for _alt in (os.path.join(BASE_DIR, "flow.db"), os.path.join(tempfile.gettempdir(), "flow.db")):
        if _db_dir_writable(_alt):
            print(f"[db] {DB_PATH} 不能寫入，改用 {_alt}")
            DB_PATH = _alt
            break
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024

# ---------- 網站存取密碼（部署到公開網址時務必設定）----------
# 登入狀態是整個伺服器共用的：沒有這層保護，任何知道網址的人都能看到你的行情與力道。
ACCESS_USER = os.getenv("ACCESS_USER", "admin")
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "")


@app.route("/healthz")
def healthz():
    return "ok"


@app.before_request
def _access():
    if not ACCESS_PASSWORD or request.path == "/healthz":
        return None
    a = request.authorization
    if a and a.username is not None and \
            hmac.compare_digest(a.username.encode(), ACCESS_USER.encode()) and \
            hmac.compare_digest((a.password or "").encode(), ACCESS_PASSWORD.encode()):
        return None
    return Response("需要存取密碼", 401, {"WWW-Authenticate": 'Basic realm="optboard"'})

# 例：TX423000K4 = 週三第4週 / 履約價 23000 / K=11月買權 / 2024
SYM_RE = re.compile(r"^(TXO|TX[1245UVXYZ])(\d{4,5})([A-X])(\d)$")

# ---------- 登入狀態（由網頁上傳憑證後才建立）----------
sdk = None
rest_stock = None
rest_fut = None
futopt_ws = None
_session = {"logged_in": False, "since": None}
_login_lock = threading.Lock()
_login_try = {"ts": 0.0}
_cert_dir = None
_threads_started = False


def _cleanup_cert():
    global _cert_dir
    if _cert_dir:
        shutil.rmtree(_cert_dir, ignore_errors=True)
        _cert_dir = None


atexit.register(_cleanup_cert)

# ---------- WebSocket trades：主力籌碼（期貨）/ 力道（選擇權）----------
# 期貨：主力籌碼 = 賣成筆(totalAskMatch) - 買成筆(totalBidMatch)
# 選擇權：力道   = 外盤量 - 內盤量；成交價 >= 成交賣價 算外盤，<= 成交買價 算內盤
# 注意：WebSocket 只能從「訂閱當下」開始累計；累計值每 15 秒寫入 SQLite，重啟後會接續今天的資料。
_flow = {}                  # key -> {"out","in","bm","am","vol","d"}
_flow_lock = threading.Lock()
_ws_wanted = set()          # {(symbol, afterHours)}
_ws_lock = threading.Lock()
_ws_state = {"connected": False, "msgs": 0, "last": None, "error": None}
_ws_reconnect = threading.Event()
_books = {}                 # key -> 最新五檔
_book_ids = {}              # key -> 頻道 id（用於退訂）
_book_req = {}              # key -> 最後被網頁查詢的時間
_books_wanted = set()       # {(symbol, afterHours)}


def _fkey(sym, after_hours):
    return f"{sym}|AH" if after_hours else sym


def auto_night(now=None):
    """依時間判斷目前是否為期貨夜盤（15:00 ~ 隔天 05:00）。只用在沒有網頁指定時段的情況（啟動預訂閱）。"""
    t = (now or dt.datetime.now()).time()
    return t >= dt.time(15, 0) or t < dt.time(5, 0)


def _ah_day():
    """夜盤的「場次日」= 該場夜盤 15:00 開始的那一天，跨午夜後仍算同一場；到下一個 15:00 才換新的一場。"""
    n = dt.datetime.now()
    return (n.date() - dt.timedelta(days=1) if n.hour < 15 else n.date()).isoformat()


_ah_meta = {"day": None, "since": None}


def _mark_ah_since():
    d = _ah_day()
    if _ah_meta["day"] != d:
        _ah_meta.update(day=d, since=time.strftime("%H:%M:%S"))


def on_ws_message(message):
    try:
        m = json.loads(message) if isinstance(message, (str, bytes)) else message
    except Exception:  # noqa
        return
    ev = m.get("event")
    if ev == "error":
        _ws_state["error"] = str(m.get("data"))
        return
    if ev == "data" and m.get("channel") == "books":
        d = m.get("data") or {}
        if d.get("symbol"):
            key = _fkey(d["symbol"], "_AH" in (d.get("type") or ""))
            _books[key] = {"bids": d.get("bids") or [], "asks": d.get("asks") or [],
                           "dBid": d.get("derivedBid"), "dAsk": d.get("derivedAsk"),
                           "trial": bool(d.get("isTrial")), "time": d.get("time"), "ts": time.time()}
            if m.get("id"):
                _book_ids[key] = m["id"]
        return
    if ev != "data" or m.get("channel") != "trades":
        return
    d = m.get("data") or {}
    if d.get("isTrial"):  # 試撮不計入
        return
    sym = d.get("symbol")
    if not sym:
        return
    key = _fkey(sym, "_AH" in (d.get("type") or ""))
    with _flow_lock:
        f = _flow.setdefault(key, {"out": 0, "in": 0, "bm": None, "am": None, "vol": None, "d": None})
        today = _ah_day() if key.endswith("|AH") else dt.date.today().isoformat()
        if f.get("d") != today:            # 新的一天（夜盤則是新的一場）：力道歸零重算
            f["out"] = f["in"] = 0
            f["d"] = today
        for t in d.get("trades") or []:
            p, sz, bid, ask = t.get("price"), t.get("size"), t.get("bid"), t.get("ask")
            if not p or not sz:
                continue
            if ask and p >= ask:
                f["out"] += sz
            elif bid and p <= bid:
                f["in"] += sz
        tot = d.get("total") or {}
        if tot.get("totalBidMatch") is not None:
            f["bm"] = tot["totalBidMatch"]
        if tot.get("totalAskMatch") is not None:
            f["am"] = tot["totalAskMatch"]
        if tot.get("tradeVolume") is not None:
            f["vol"] = tot["tradeVolume"]
    _ws_state["msgs"] += 1
    _ws_state["last"] = time.strftime("%H:%M:%S")


def _ws_send(symbols, after_hours, channel="trades"):
    for i in range(0, len(symbols), 50):
        part = symbols[i:i + 50]
        params = {"channel": channel}
        if len(part) == 1:
            params["symbol"] = part[0]
        else:
            params["symbols"] = part
        if after_hours:
            params["afterHours"] = True
        try:
            futopt_ws.subscribe(params)
        except Exception as e:  # noqa
            _ws_state["error"] = f"subscribe: {type(e).__name__}: {e}"


def ws_subscribe(symbols, after_hours=False):
    """冪等：已訂閱過的不會重複送出；尚未連線時先記下，連線後補訂。超過 WS_MAX 就不再加訂。"""
    with _ws_lock:
        new = []
        for x in symbols:
            if not x or (x, after_hours) in _ws_wanted:
                continue
            if len(_ws_wanted) >= WS_MAX:
                break
            _ws_wanted.add((x, after_hours))
            new.append(x)
        if new and _ws_state["connected"]:
            _ws_send(new, after_hours)


def _ws_connect_once():
    futopt_ws.connect()
    _ws_state["connected"] = True
    _ws_state["error"] = None
    with _ws_lock:
        for ah in (False, True):
            syms = [x for x, a in _ws_wanted if a == ah]
            if syms:
                _ws_send(syms, ah)
            bs = [x for x, a in _books_wanted if a == ah]
            if bs:
                _ws_send(bs, ah, "books")


def _on_ws_disconnect(code=None, message=None, *reason):
    _ws_state["connected"] = False
    _ws_state["error"] = f"disconnect: {code} {message}"
    if _session["logged_in"]:
        _ws_reconnect.set()


def _on_ws_error(error):
    _ws_state["error"] = f"error: {error}"


def _ws_reconnect_loop():
    retries = 0
    while _ws_reconnect.wait():
        _ws_reconnect.clear()
        if not _session["logged_in"]:
            continue
        retries += 1
        time.sleep(min(2 ** retries, 30))
        try:
            try:
                futopt_ws.disconnect()
            except Exception:  # noqa
                pass
            _ws_connect_once()
            retries = 0
        except Exception as e:  # noqa
            _ws_state["error"] = f"reconnect: {type(e).__name__}: {e}"
            _ws_reconnect.set()


def fut_flow(sym, after_hours):
    with _flow_lock:
        f = _flow.get(_fkey(sym, after_hours))
        if not f or f["am"] is None or f["bm"] is None:
            return None
        return {"ask": f["am"], "bid": f["bm"], "net": f["am"] - f["bm"]}


def opt_flow(sym, ah=False):
    day = _ah_day() if ah else dt.date.today().isoformat()
    with _flow_lock:
        f = _flow.get(_fkey(sym, ah))
        o, i = (f["out"], f["in"]) if f and f.get("d") == day else (None, None)
    if o is None:
        with _ws_lock:
            if (sym, ah) not in _ws_wanted:
                return None, None, None
        o = i = 0
    return o - i, o, i


def ws_subscribe_books(sym, after_hours=False):
    key = _fkey(sym, after_hours)
    with _ws_lock:
        _book_req[key] = time.time()
        if (sym, after_hours) in _books_wanted:
            return
        _books_wanted.add((sym, after_hours))
        if len(_books_wanted) > BOOK_MAX:          # 超過上限：退訂最久沒人看的
            for sy, ah in sorted(_books_wanted, key=lambda x: _book_req.get(_fkey(*x), 0)):
                k = _fkey(sy, ah)
                bid = _book_ids.get(k)
                if (sy, ah) == (sym, after_hours) or not bid or time.time() - _book_req.get(k, 0) < 30:
                    continue
                try:
                    futopt_ws.unsubscribe({"id": bid})
                except Exception:  # noqa
                    continue
                _books_wanted.discard((sy, ah))
                _books.pop(k, None)
                _book_ids.pop(k, None)
                if len(_books_wanted) <= BOOK_MAX:
                    break
        if _ws_state["connected"]:
            _ws_send([sym], after_hours, "books")


# ---------- SQLite：力道資料保存 ----------
# 為什麼用 SQLite：Python 內建、零設定、單一檔案好備份；本程式本來就是單行程 + 每 15 秒批次寫入，
# 資料量約「每天 數百~數千筆」，SQLite 綽綽有餘。若之後要多人 / 多主機 / 長期大量歷史分析，再換 PostgreSQL。
_db_lock = threading.Lock()
_db_state = {"error": None}
_flow_meta = {"since": None}


def _db():
    return sqlite3.connect(DB_PATH, timeout=10)


def db_init():
    with _db_lock, closing(_db()) as c:
        try:
            c.execute("PRAGMA journal_mode=WAL")
        except Exception:  # noqa
            pass
        c.execute("""CREATE TABLE IF NOT EXISTS opt_flow(
            day TEXT NOT NULL, symbol TEXT NOT NULL, strike INTEGER NOT NULL, cp TEXT NOT NULL,
            out_vol INTEGER NOT NULL DEFAULT 0, in_vol INTEGER NOT NULL DEFAULT 0, vol INTEGER,
            updated TEXT, PRIMARY KEY(day, symbol))""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_opt_flow ON opt_flow(day, strike, cp)")
        c.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        cut = (dt.date.today() - dt.timedelta(days=DB_KEEP_DAYS)).isoformat()
        c.execute("DELETE FROM opt_flow WHERE day < ?", (cut,))
        c.commit()


def _flow_load():
    """啟動時把今天日盤 / 目前這一場夜盤已存的力道讀回記憶體，接續累計。"""
    today, ahd = dt.date.today().isoformat(), _ah_day()
    try:
        with _db_lock, closing(_db()) as c:
            row = c.execute("SELECT v FROM meta WHERE k=?", (f"since:{today}",)).fetchone()
            _flow_meta["since"] = row[0] if row else None
            rows = c.execute(
                "SELECT day,symbol,out_vol,in_vol,vol FROM opt_flow "
                "WHERE (day=? AND symbol NOT LIKE '%|AH') OR (day=? AND symbol LIKE '%|AH')", (today, ahd)).fetchall()
        with _flow_lock:
            for day, sym, o, i, v in rows:
                _flow[sym] = {"out": o, "in": i, "bm": None, "am": None, "vol": v, "d": day}
    except Exception as e:  # noqa
        _db_state["error"] = f"load: {type(e).__name__}: {e}"


def _mark_since():
    """登入成功後，記錄今天從幾點開始累計（只記第一次）。"""
    today = dt.date.today().isoformat()
    now = time.strftime("%H:%M:%S")
    try:
        with _db_lock, closing(_db()) as c:
            c.execute("INSERT OR IGNORE INTO meta(k,v) VALUES(?,?)", (f"since:{today}", now))
            c.commit()
            row = c.execute("SELECT v FROM meta WHERE k=?", (f"since:{today}",)).fetchone()
        _flow_meta["since"] = row[0] if row else now
    except Exception as e:  # noqa
        _db_state["error"] = f"meta: {type(e).__name__}: {e}"
        _flow_meta["since"] = _flow_meta["since"] or now


def _flow_snapshot():
    today, ahd = dt.date.today().isoformat(), _ah_day()
    snap = {}
    with _flow_lock:
        for k, v in _flow.items():
            ah = k.endswith("|AH")
            if v.get("d") != (ahd if ah else today) or not (v["out"] or v["in"] or v.get("vol")):
                continue
            m = SYM_RE.match(k[:-3] if ah else k)
            if not m:
                continue
            snap[k] = (int(m.group(2)), "C" if m.group(3) <= "L" else "P", v["out"], v["in"], v.get("vol"), v["d"])
    return snap


def flow_save(last=None):
    snap = _flow_snapshot()
    key = tuple(sorted(snap.items()))
    if last == key:
        return last
    now = time.strftime("%H:%M:%S")
    try:
        with _db_lock, closing(_db()) as c:
            c.executemany(
                """INSERT INTO opt_flow(day,symbol,strike,cp,out_vol,in_vol,vol,updated) VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(day,symbol) DO UPDATE SET out_vol=excluded.out_vol, in_vol=excluded.in_vol,
                   vol=excluded.vol, updated=excluded.updated""",
                [(v[5], s, v[0], v[1], v[2], v[3], v[4], now) for s, v in snap.items()])
            c.commit()
        _db_state["error"] = None
        _db_state["saved"] = now
        return key
    except Exception as e:  # noqa
        _db_state["error"] = f"save: {type(e).__name__}: {e}"
        return last


def _flow_saver():
    last = None
    while True:
        time.sleep(15)
        if _session["logged_in"]:
            last = flow_save(last)


def week_start(d=None):
    d = d or dt.date.today()
    return d - dt.timedelta(days=d.weekday())


def _next_bd(d):
    d += dt.timedelta(days=1)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d


def trade_day(day_id, ah):
    """力道資料的「交易日」：日盤 = 當天；夜盤(15:00 起)屬於下一個交易日。"""
    d = dt.date.fromisoformat(day_id)
    return _next_bd(d) if ah else d


def current_trade_day():
    n = dt.datetime.now()
    if n.hour >= 15 or n.hour < 5:           # 夜盤時段
        return trade_day(_ah_day(), True)
    return n.date()


def _in_scope(day_id, ah, scope, T):
    td = trade_day(day_id, ah)
    return td == T if scope == "day" else week_start(T) <= td <= T


def acc_map(scope):
    """各履約價（跨所有到期別）的外盤 / 內盤累積：{(strike, 'C'|'P'): [out, in]}
    累積 = 「已存入資料庫的前幾場」＋「目前這一場的即時值」；場次一換，力道歸零重算，但前面已存的會留在累積裡。
    scope = day   當前交易日（前一晚夜盤 + 當天日盤）
    scope = week  本週（週一交易日起，含週五夜盤）
    實作：資料庫各場次的值，被記憶體裡同一場次的即時值蓋掉（= 上次儲存的基準 + 之後增加的差值），再依範圍加總。"""
    T = current_trade_day()
    lo = (week_start(T) if scope == "week" else T) - dt.timedelta(days=4)
    cells = {}                                   # (場次日, key) -> (strike, cp, out, in)
    try:
        with _db_lock, closing(_db()) as c:
            for day, sym, strike, cp, o, i in c.execute(
                    "SELECT day,symbol,strike,cp,out_vol,in_vol FROM opt_flow WHERE day>=?", (lo.isoformat(),)):
                cells[(day, sym)] = (strike, cp, o or 0, i or 0)
    except Exception as e:  # noqa
        _db_state["error"] = f"acc: {type(e).__name__}: {e}"
    with _flow_lock:
        for k, v in _flow.items():
            ah = k.endswith("|AH")
            mm = SYM_RE.match(k[:-3] if ah else k)
            if mm and v.get("d"):
                cells[(v["d"], k)] = (int(mm.group(2)), "C" if mm.group(3) <= "L" else "P", v["out"], v["in"])
    m = {}
    for (day, sym), (strike, cp, o, i) in cells.items():
        if _in_scope(day, sym.endswith("|AH"), scope, T):
            a = m.setdefault((strike, cp), [0, 0])
            a[0] += o
            a[1] += i
    return m


# ---------- 登入 / 登出 ----------
def _setup_ws():
    futopt_ws.on("message", on_ws_message)
    futopt_ws.on("disconnect", _on_ws_disconnect)
    futopt_ws.on("error", _on_ws_error)
    try:
        _ws_connect_once()
    except Exception as e:  # noqa
        _ws_state["error"] = f"connect: {type(e).__name__}: {e}"
        _ws_reconnect.set()


def _start_background():
    global _threads_started
    if not _threads_started:
        _threads_started = True
        threading.Thread(target=_ws_reconnect_loop, daemon=True).start()
        threading.Thread(target=_flow_saver, daemon=True).start()
    threading.Thread(target=flow_bootstrap, daemon=True).start()


def do_login(uid, pwd, cert_bytes, cert_pwd):
    global sdk, rest_stock, rest_fut, futopt_ws, _cert_dir
    with _login_lock:
        _cleanup_cert()
        d = tempfile.mkdtemp(prefix="fubon_")
        try:
            os.chmod(d, 0o700)
        except Exception:  # noqa
            pass
        path = os.path.join(d, "cert.p12")
        with open(path, "wb") as fp:
            fp.write(cert_bytes)
        try:
            os.chmod(path, 0o600)
        except Exception:  # noqa
            pass
        _cert_dir = d      # 憑證只存在這個暫存資料夾；登出或程式結束時刪除
        try:
            s = FubonSDK()
            acc = s.login(uid, pwd, path, cert_pwd)
            if hasattr(acc, "is_success") and not acc.is_success:
                raise RuntimeError(getattr(acc, "message", None) or str(acc))
            s.init_realtime()
        except Exception:
            _cleanup_cert()
            raise
        sdk = s
        rest_stock = s.marketdata.rest_client.stock
        rest_fut = s.marketdata.rest_client.futopt
        futopt_ws = s.marketdata.websocket_client.futopt
        _session.update(logged_in=True, since=time.strftime("%H:%M:%S"))
        _setup_ws()
        _mark_since()
        _start_background()


def env_login_ready():
    """.env 裡帳號 / 密碼 / 憑證路徑都有填，而且憑證檔真的存在。"""
    return bool(ENV_ID and ENV_PWD and (ENV_CERT_B64 or (ENV_CERT_PATH and os.path.isfile(_env_cert_path()))))


def _env_cert_path():
    p = os.path.expanduser(ENV_CERT_PATH)
    return p if os.path.isabs(p) else os.path.join(BASE_DIR, p)


def login_from_env():
    if ENV_CERT_B64:
        data = base64.b64decode(ENV_CERT_B64)
    else:
        with open(_env_cert_path(), "rb") as fp:
            data = fp.read(MAX_CERT_BYTES + 1)
    if not data or len(data) > MAX_CERT_BYTES:
        raise RuntimeError("憑證檔大小不正確（應為 .p12 / .pfx，小於 64KB）")
    do_login(ENV_ID, ENV_PWD, data, ENV_CERT_PWD)


@app.before_request
def _guard():
    p = request.path
    if p.startswith("/api/") and p not in ("/api/login", "/api/login/env", "/api/status") and not _session["logged_in"]:
        return jsonify({"ok": False, "needLogin": True, "error": "尚未登入，請先上傳憑證登入"}), 401


@app.route("/api/status")
def status():
    info = {"loggedIn": _session["logged_in"], "since": _session["since"], "envLogin": env_login_ready()}
    if _session["logged_in"]:
        try:
            with _db_lock, closing(_db()) as c:
                info["dbDays"] = c.execute("SELECT COUNT(DISTINCT day) FROM opt_flow").fetchone()[0]
                info["dbRowsToday"] = c.execute("SELECT COUNT(*) FROM opt_flow WHERE day=?",
                                                (dt.date.today().isoformat(),)).fetchone()[0]
        except Exception as e:  # noqa
            info["dbError"] = f"{type(e).__name__}: {e}"
        info["dbFile"] = os.path.basename(DB_PATH)
    return jsonify(info)


@app.route("/api/login", methods=["POST"])
def login():
    uid = (request.form.get("id") or "").strip().upper()
    pwd = request.form.get("pwd") or ""
    cpwd = request.form.get("cert_pwd") or ""
    f = request.files.get("cert")
    if not uid or not pwd or f is None:
        return jsonify({"ok": False, "error": "請填寫身分證字號、密碼並選擇憑證檔"}), 400
    data = f.read(MAX_CERT_BYTES + 1)
    if not data or len(data) > MAX_CERT_BYTES:
        return jsonify({"ok": False, "error": "憑證檔大小不正確（應為 .p12 / .pfx，小於 64KB）"}), 400
    if time.time() - _login_try["ts"] < 3:           # 避免連續錯誤嘗試造成帳號鎖定
        return jsonify({"ok": False, "error": "請稍候幾秒再試"}), 429
    _login_try["ts"] = time.time()
    try:
        do_login(uid, pwd, data, cpwd)
    except Exception as e:  # noqa
        return jsonify({"ok": False, "error": f"登入失敗：{e}"}), 400
    return jsonify({"ok": True})


@app.route("/api/login/env", methods=["POST"])
def login_env():
    """用 .env 的 FUBON_ID / FUBON_PWD / FUBON_CERT_PATH / FUBON_CERT_PWD 登入。"""
    if not env_login_ready():
        return jsonify({"ok": False, "error": ".env 尚未設定完整（需要 FUBON_ID、FUBON_PWD，以及 FUBON_CERT_PATH 或 FUBON_CERT_B64）"}), 400
    if _session["logged_in"]:
        return jsonify({"ok": True, "already": True})
    if time.time() - _login_try["ts"] < 3:
        return jsonify({"ok": False, "error": "請稍候幾秒再試"}), 429
    _login_try["ts"] = time.time()
    try:
        login_from_env()
    except Exception as e:  # noqa
        return jsonify({"ok": False, "error": f"登入失敗：{e}"}), 400
    return jsonify({"ok": True})


@app.route("/api/logout", methods=["POST"])
def logout():
    with _login_lock:
        _session["logged_in"] = False
        flow_save()
        try:
            futopt_ws.disconnect()
        except Exception:  # noqa
            pass
        try:
            sdk.logout()
        except Exception:  # noqa
            pass
        _ws_state["connected"] = False
        _cleanup_cert()
    return jsonify({"ok": True})


# ---------- 儲存 / 清除力道 ----------
@app.route("/api/flow/save", methods=["POST"])
def flow_save_now():
    """立刻把目前的力道寫入 SQLite（平常每 15 秒會自動存一次）。"""
    flow_save(None)
    if _db_state["error"]:
        return jsonify({"ok": False, "error": _db_state["error"]}), 500
    _chain_cache.clear()          # 讓接著刷新的選擇權鏈重新計算累積
    return jsonify({"ok": True, "time": _db_state.get("saved") or time.strftime("%H:%M:%S")})


@app.route("/api/flow/clear", methods=["POST"])
def flow_clear():
    """清除選擇權力道累積（外盤 / 內盤歸零，總量不動）。
    scope = day  清當前交易日（前一晚夜盤 + 當天日盤）；scope = week 清本週（週一交易日起）全部，含資料庫。"""
    scope = "day" if (request.get_json(silent=True) or {}).get("scope") == "day" else "week"
    today = dt.date.today().isoformat()
    T = current_trade_day()
    with _flow_lock:
        for k, v in _flow.items():
            ah = k.endswith("|AH")
            if SYM_RE.match(k[:-3] if ah else k) and v.get("d") and _in_scope(v["d"], ah, scope, T):
                v["out"] = v["in"] = 0
    now = time.strftime("%H:%M:%S")
    lo = (week_start(T) if scope == "week" else T) - dt.timedelta(days=4)
    try:
        with _db_lock, closing(_db()) as c:
            dels = [(d, s_) for d, s_ in c.execute("SELECT day,symbol FROM opt_flow WHERE day>=?", (lo.isoformat(),))
                    if _in_scope(d, s_.endswith("|AH"), scope, T)]
            c.executemany("DELETE FROM opt_flow WHERE day=? AND symbol=?", dels)
            c.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", (f"since:{today}", now))
            c.commit()
        _db_state["error"] = None
    except Exception as e:  # noqa
        _db_state["error"] = f"clear: {type(e).__name__}: {e}"
        return jsonify({"ok": False, "error": _db_state["error"]}), 500
    _flow_meta["since"] = now
    with _chain_lock:
        _chain_cache.clear()
    return jsonify({"ok": True, "scope": scope, "since": now})


# ---------- 節流：期權行情 REST ----------
_rl_lock = threading.Lock()
_calls = deque()
_state = {"block_until": 0.0}


def gate():
    """確保最近 60 秒內的期權行情請求數不超過上限。"""
    while True:
        with _rl_lock:
            now = time.time()
            while _calls and now - _calls[0] > 60:
                _calls.popleft()
            if len(_calls) < FUT_MAX_PER_MIN:
                _calls.append(now)
                return
            wait = 60 - (now - _calls[0]) + 0.05
        time.sleep(max(wait, 0.05))


def fcall(fn, **kw):
    gate()
    return fn(**kw)


# ---------- 共用工具 ----------
def _last_price(q):
    """最後成交價；沒有成交時退回 lastTrade / closePrice。"""
    p = q.get("lastPrice")
    if p is None:
        p = q.get("closePrice")
    if p is None:
        p = (q.get("lastTrade") or {}).get("price")
    return p


def simplify(q, label=None):
    return {
        "ok": True,
        "name": q.get("name") or label,
        "symbol": q.get("symbol"),
        "price": _last_price(q),
        "open": q.get("openPrice"),
        "high": q.get("highPrice"),
        "low": q.get("lowPrice"),
        "previousClose": q.get("previousClose"),
        "avgPrice": q.get("avgPrice"),
        "change": q.get("change"),
        "changePercent": q.get("changePercent"),
        "volume": (q.get("total") or {}).get("tradeVolume"),
        "updated": q.get("lastUpdated", 0),
    }


def safe(fn):
    try:
        return fn()
    except FugleAPIError as e:
        return {"ok": False, "error": f"{e.status_code} {e.response_text}"}
    except Exception as e:  # noqa
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# ---------- 指數 ----------
_index_cache = {}


def resolve_index(key, exchange, keywords, fallback):
    if key in _index_cache:
        return _index_cache[key]
    best = None
    try:
        res = rest_stock.intraday.tickers(type="INDEX", exchange=exchange)
        for r in res.get("data", []):
            nm = r.get("name", "")
            if not any(k in nm for k in keywords):
                continue
            score = 10 - (8 if "報酬" in nm else 0) - (5 if ("不含" in nm or "未含" in nm) else 0)
            if best is None or score > best[0]:
                best = (score, r["symbol"], nm)
    except Exception:
        pass
    _index_cache[key] = (best[1], best[2]) if best else (fallback, None)
    return _index_cache[key]


def get_index(key, label, exchange, keywords, fallback):
    sym, nm = resolve_index(key, exchange, keywords, fallback)
    d = simplify(rest_stock.intraday.quote(symbol=sym), label)
    d["symbol"] = sym
    d["name"] = label
    return d


def get_stock():
    d = simplify(rest_stock.intraday.quote(symbol=STOCK_SYMBOL))
    return d


# ---------- 台指期近月 ----------
_txf = {"date": None, "symbol": None}


def pick_near_month_txf():
    today = dt.date.today().isoformat()
    if _txf["date"] == today and _txf["symbol"]:
        return _txf["symbol"]
    res = fcall(rest_fut.intraday.tickers, type="FUTURE", exchange="TAIFEX",
                session="REGULAR", product="TXF")
    rows = [r for r in res.get("data", [])
            if r.get("symbol", "").startswith("TXF") and "/" not in r.get("symbol", "")
            and r.get("settlementDate", "9999") >= today]
    if not rows:
        raise RuntimeError("找不到可用的台指期合約")
    rows.sort(key=lambda r: r["settlementDate"])
    _txf.update(date=today, symbol=rows[0]["symbol"])
    return _txf["symbol"]


def quote_fut(session=None):
    """優先用連續月別名 TXF1! 查近月台指期；別名失敗(非429)才退回 tickers 查合約。"""
    kw = {"session": session} if session else {}
    try:
        return fcall(rest_fut.intraday.quote, symbol=FUT_ALIAS, **kw)
    except FugleAPIError as e:
        if e.status_code == 429:
            _state["block_until"] = time.time() + 60
            raise
    except Exception:  # noqa
        pass
    return fcall(rest_fut.intraday.quote, symbol=pick_near_month_txf(), **kw)


def _fut_one(session, label):
    q = simplify(quote_fut(session))
    q["name"] = "台指近月"
    q["symbol"] = q.get("symbol") or FUT_ALIAS
    q["session"] = label
    ah = session == "afterhours"
    sym = q.get("symbol")
    if sym and sym != FUT_ALIAS:
        ws_subscribe([sym], ah)
    q["flow"] = fut_flow(sym, ah)
    return q


def get_fut():
    """同時回傳日盤與夜盤報價，由前端決定顯示哪一個；auto 為『最後更新較新』的時段。"""
    day = safe(lambda: _fut_one(None, "日盤"))
    night = safe(lambda: _fut_one("afterhours", "夜盤"))
    auto = "day"
    if night.get("ok") and (not day.get("ok") or (night.get("updated") or 0) > (day.get("updated") or 0)):
        auto = "night"
    err = None
    if not day.get("ok") and not night.get("ok"):
        err = day.get("error") or night.get("error")
    return {"ok": bool(day.get("ok") or night.get("ok")), "error": err,
            "day": day, "night": night, "auto": auto}


# ---------- 選擇權合約表 ----------
OPTION_PRODUCTS = ["TXO", "TX1", "TX2", "TX4", "TX5", "TXU", "TXV", "TXX", "TXY", "TXZ"]
_opt = {"date": None, "expiries": {}, "errors": []}
_opt_lock = threading.Lock()


def load_option_contracts():
    today = dt.date.today().isoformat()
    with _opt_lock:
        if _opt["date"] == today and _opt["expiries"]:
            return _opt["expiries"]
        expiries, errors = {}, []
        for prod in OPTION_PRODUCTS:
            try:
                res = fcall(rest_fut.intraday.tickers, type="OPTION", exchange="TAIFEX",
                            session="REGULAR", product=prod)
            except Exception as e:  # noqa
                errors.append(f"{prod}: {type(e).__name__} {e}")
                continue
            for r in res.get("data", []):
                m = SYM_RE.match(r.get("symbol", ""))
                settle = r.get("settlementDate") or r.get("endDate")
                if not m or not settle or settle < today:
                    continue
                code, strike, mc, _yd = m.groups()
                cp = "C" if mc <= "L" else "P"  # A-L 買權 / M-X 賣權
                eid = f"{code}|{settle}"
                e = expiries.setdefault(eid, {"id": eid, "product": code, "settle": settle, "strikes": {}})
                e["strikes"].setdefault(int(strike), {})[cp] = r["symbol"]
        _opt.update(date=today, expiries=expiries, errors=errors)
        return expiries


def expiry_label(e):
    d = dt.date.fromisoformat(e["settle"])
    if e["product"] == "TXO":
        return f"{d.year}/{d.month:02d}"
    nth = (d.day - 1) // 7 + 1
    kind = "W" if d.weekday() == 2 else "F" if d.weekday() == 4 else "?"
    return f"{d.month:02d}{kind}{nth}"


def is_settled(e):
    return e["settle"] == dt.date.today().isoformat() and dt.datetime.now().time() >= dt.time(13, 30)


def selected_expiries():
    """月選近月 1 個、週三 2 個、週五 2 個；當天將結算 / 已結算的仍保留（當天收盤後才會消失）。"""
    lst = sorted(load_option_contracts().values(), key=lambda x: (x["settle"], x["product"]))
    wd = lambda e: dt.date.fromisoformat(e["settle"]).weekday()
    month = [e for e in lst if e["product"] == "TXO"][:1]
    wed = [e for e in lst if e["product"] != "TXO" and wd(e) == 2][:2]
    fri = [e for e in lst if e["product"] != "TXO" and wd(e) == 4][:2]
    return sorted(month + wed + fri, key=lambda x: (x["settle"], x["product"]))


@app.route("/api/expiries")
def expiries():
    today = dt.date.today()
    out = []
    for e in selected_expiries():
        d = dt.date.fromisoformat(e["settle"])
        days = (d - today).days
        settled = is_settled(e)
        sub = f"{d.month:02d}-{d.day:02d} ({'已結算' if settled else f'{days}天'})"
        out.append({"id": e["id"], "label": expiry_label(e), "sub": sub, "settle": e["settle"],
                    "settled": settled, "strikes": len(e["strikes"])})
    return jsonify({"expiries": out, "errors": _opt["errors"]})


# ---------- 選擇權鏈 ----------
_chain_cache = {}
_chain_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=5)
_optq = {}      # symbol -> (ts, 原始報價)；「全部履約價」模式下，遠端履約價用較長的間隔更新


def fetch_opt(sym, ttl=0.0, ah=False):
    """選擇權單一合約報價：intraday/quote/{symbol}（與期貨同一支 API）。ttl>0 時，在 ttl 秒內重用上次的 REST 結果。
    ah=True 時查夜盤（session=afterhours）。"""
    if not sym:
        return None
    qkey = _fkey(sym, ah)
    c = _optq.get(qkey)
    if c and ttl and time.time() - c[0] < ttl:
        raw = c[1]
    else:
        try:
            q = fcall(rest_fut.intraday.quote, symbol=sym, **({"session": "afterhours"} if ah else {}))
        except FugleAPIError as e:
            if e.status_code == 429:
                _state["block_until"] = time.time() + 60
            return {"symbol": sym, "error": f"{e.status_code} {e.response_text}"}
        except Exception as e:  # noqa
            return {"symbol": sym, "error": f"{type(e).__name__}: {e}"}
        raw = {
            "symbol": sym,
            "price": _last_price(q),
            "open": q.get("openPrice"),
            "high": q.get("highPrice"),
            "low": q.get("lowPrice"),
            "change": q.get("change"),
            "changePercent": q.get("changePercent"),
            "volume": (q.get("total") or {}).get("tradeVolume"),
        }
        _optq[qkey] = (time.time(), raw)
    flow, fo, fi = opt_flow(sym, ah)
    return {**raw, "flow": flow, "out": fo, "in": fi}


def expiry_totals(e, ah=False):
    """該到期別「全部履約價」的總量與力道加總（全市場）。
    n  = 已訂閱力道的合約數、vn = 有總量資料的合約數、of = 該邊合約總數。"""
    today = _ah_day() if ah else dt.date.today().isoformat()
    t = {cp: {"vol": 0, "out": 0, "in": 0, "n": 0, "vn": 0, "of": 0} for cp in ("C", "P")}
    with _ws_lock:
        wanted = {s for s, a in _ws_wanted if a == ah}
    with _flow_lock:
        for sd in e["strikes"].values():
            for cp, sym in sd.items():
                tt = t[cp]
                tt["of"] += 1
                f = _flow.get(_fkey(sym, ah))
                if f is not None and f.get("d") != today:
                    f = None
                q = _optq.get(_fkey(sym, ah))
                vol = max((q[1].get("volume") or 0) if q else 0, (f.get("vol") or 0) if f else 0)
                if q or (f and f.get("vol") is not None):
                    tt["vn"] += 1
                tt["vol"] += vol
                if f:
                    tt["out"] += f["out"]
                    tt["in"] += f["in"]
                if sym in wanted or f:
                    tt["n"] += 1
    for tt in t.values():
        tt["flow"] = tt["out"] - tt["in"]
    return t


def _fut_price(f, ah=None):
    """ah=None 用 auto 時段；ah=True 夜盤；ah=False 日盤。"""
    if f and f.get("ok"):
        q = f.get(f.get("auto") if ah is None else ("night" if ah else "day"))
        if q and q.get("ok") and q.get("price"):
            return q["price"]
    return None


def center_price(ah=None):
    p = _fut_price((_cache.get("data") or {}).get("fut"), ah)
    return p if p else _fut_price(safe(get_fut), ah)


@app.route("/api/chain")
def chain():
    eid = request.args.get("expiry", "")
    w = request.args.get("win", "")
    if w == "all":
        win = None
    else:
        try:
            win = max(1, min(int(w), 80))
        except ValueError:
            win = OPTION_WINDOW
    acc = "day" if request.args.get("acc") == "day" else "week"   # 預設週累積
    ah = request.args.get("ah") == "1"      # 跟著台指期的日盤 / 夜盤切換
    exps = load_option_contracts()
    e = exps.get(eid)
    if not e:
        return jsonify({"ok": False, "error": "找不到此到期別"}), 404

    ckey = (eid, win, acc, ah)
    with _chain_lock:
        c = _chain_cache.get(ckey)
        if c and time.time() - c["ts"] < CHAIN_TTL:
            return jsonify(c["data"])
        if time.time() < _state["block_until"] and c:
            d = dict(c["data"])
            d["warning"] = "已達速率限制(429)，暫時顯示舊資料，約 1 分鐘後恢復"
            return jsonify(d)

        center = center_price(ah)
        strikes = sorted(e["strikes"])
        if center:
            idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - center))
        else:
            idx = len(strikes) // 2
        lo, hi = (0, len(strikes)) if win is None else (max(0, idx - win), min(len(strikes), idx + win + 1))
        chosen = strikes[lo:hi]
        atm = strikes[idx]

        # 力道 / 總量要看全市場：整個到期別的履約價都訂閱 trades（由價平往外，超過 WS_MAX 的最遠幾檔不訂）
        if not is_settled(e):
            order = sorted(range(len(strikes)), key=lambda i: abs(i - idx))
            ws_subscribe([e["strikes"][strikes[i]].get(cp) for i in order for cp in ("C", "P")], ah)
            if ah:
                _mark_ah_since()

        near = OPTION_WINDOW if win is None else 10 ** 6   # 全部模式：價平附近照常更新，遠端用 FAR_TTL
        jobs = []
        for j in range(lo, hi):
            ttl = 0.0 if abs(j - idx) <= near else FAR_TTL
            jobs += [(e["strikes"][strikes[j]].get("C"), ttl, ah), (e["strikes"][strikes[j]].get("P"), ttl, ah)]
        results = list(_pool.map(lambda a: fetch_opt(*a), jobs))

        accm = acc_map(acc)

        def acc_cell(k, cp):
            a = accm.get((k, cp))
            return {"net": a[0] - a[1], "out": a[0], "in": a[1]} if a else None

        rows, errs = [], 0
        for i, k in enumerate(chosen):
            call, put = results[2 * i], results[2 * i + 1]
            for x in (call, put):
                if x and x.get("error"):
                    errs += 1
            rows.append({"strike": k, "C": call, "P": put,
                         "acc": {"C": acc_cell(k, "C"), "P": acc_cell(k, "P")}})

        totals = expiry_totals(e, ah)
        totals["accC"] = sum(a[0] - a[1] for (k, cp), a in accm.items() if cp == "C")
        totals["accP"] = sum(a[0] - a[1] for (k, cp), a in accm.items() if cp == "P")
        totals["accScope"] = acc

        data = {"ok": True, "id": eid, "label": expiry_label(e), "settle": e["settle"],
                "center": center, "atm": atm, "rows": rows, "errors": errs,
                "win": "all" if win is None else win, "shown": len(chosen), "strikesTotal": len(strikes),
                "totals": totals,
                "session": "night" if ah else "day",
                "flowSince": (_ah_meta["since"] if ah else _flow_meta["since"]) or "--",
                "dbError": _db_state["error"], "savedAt": _db_state.get("saved"),
                "updated": time.strftime("%H:%M:%S")}
        if errs and time.time() < _state["block_until"]:
            data["warning"] = "部分請求遭速率限制(429)，約 1 分鐘後恢復"
        _chain_cache[ckey] = {"ts": time.time(), "data": data}
        return jsonify(data)


# ---------- 上方報價列 ----------
_cache = {"ts": 0, "data": None}
_lock = threading.Lock()


@app.route("/api/quotes")
def quotes():
    with _lock:
        if _cache["data"] and time.time() - _cache["ts"] < 2:
            return jsonify(_cache["data"])
        data = {
            "twse": safe(lambda: get_index("twse", "加權指數", "TWSE", ["加權"], TWSE_FALLBACK)),
            "tpex": safe(lambda: get_index("tpex", "櫃買指數", "TPEx", ["櫃買", "櫃檯"], TPEX_FALLBACK)),
            "fut": safe(get_fut),
            "serverTime": time.strftime("%H:%M:%S"),
        }
        _cache.update(ts=time.time(), data=data)
        return jsonify(data)


TF_DAYS = {"5": 30, "15": 90, "60": 120, "D": 360}   # 各週期往回抓幾天（單次區間須小於 1 年）
_candle_cache = {}


def _tconv(s, daily):
    if daily:
        return s[:10]
    return int(dt.datetime.fromisoformat(s).timestamp()) + 8 * 3600   # 圖表以 UTC 顯示，所以先加 8 小時


def _fetch_one(tf, sess):
    """單一時段（regular / afterhours）的 K 線：historical + intraday 合併。回傳 (bars, error)。"""
    now = dt.datetime.now()
    today = now.date()
    # 夜盤 15:00 起歸屬下一個交易日
    to_days = [1, 0] if sess == "afterhours" and now.hour >= 15 else [0]
    res, err = None, None
    for add in to_days:
        kw = {"product": "TXF", "contractMonth": "1!", "timeframe": tf, "sort": "asc",
              "from": (today - dt.timedelta(days=TF_DAYS[tf])).isoformat(),
              "to": (today + dt.timedelta(days=add)).isoformat(),
              "fields": "open,high,low,close,volume"}
        if sess == "afterhours":
            kw["session"] = "afterhours"
        try:
            res = fcall(rest_fut.historical.candles, **kw)
            break
        except FugleAPIError as e:
            if e.status_code == 404:            # 區間內查無資料
                res = {"data": []}
                break
            err = f"{e.status_code} {e.response_text}"
        except Exception as e:  # noqa
            err = f"{type(e).__name__}: {e}"
    if res is None:
        return None, err or "查詢失敗"
    daily = tf == "D"
    bars = [{"t": _tconv(x["date"], daily), "o": x.get("open"), "h": x.get("high"),
             "l": x.get("low"), "c": x.get("close"), "v": x.get("volume") or 0}
            for x in res.get("data", []) if x.get("open") is not None]
    # 以 intraday/candles 補上最新（尚未進入歷史資料庫）的 K 線
    try:
        live = fcall(rest_fut.intraday.candles, symbol=pick_near_month_txf(),
                     timeframe="5" if tf == "D" else tf,
                     **({"session": "afterhours"} if sess == "afterhours" else {}))
        lrows = [x for x in live.get("data", []) if x.get("open") is not None]
        if tf != "D":
            m = {b["t"]: b for b in bars}
            for x in lrows:
                t = _tconv(x["date"], False)
                m[t] = {"t": t, "o": x["open"], "h": x["high"], "l": x["low"],
                        "c": x["close"], "v": x.get("volume") or 0}
            bars = list(m.values())
        elif sess == "regular" and lrows:        # 日K：用今天的 5 分K 合成當天這一根
            day = (live.get("date") or today.isoformat())[:10]
            agg = {"t": day, "o": lrows[0]["open"], "h": max(x["high"] for x in lrows),
                   "l": min(x["low"] for x in lrows), "c": lrows[-1]["close"],
                   "v": sum(x.get("volume") or 0 for x in lrows)}
            bars = [b for b in bars if b["t"] != day] + [agg]
    except Exception:  # noqa  intraday 失敗就只用歷史資料
        pass
    bars.sort(key=lambda b: str(b["t"]) if daily else b["t"])
    return bars, None


def _get_candles(tf, sess):
    key = (tf, sess)
    c = _candle_cache.get(key)
    if c and time.time() - c["ts"] < 8:
        return c["data"]
    if sess == "all":
        # 富邦 API 只有日盤(預設)與夜盤(afterhours)，沒有全日盤：這裡把兩個時段的 K 線依時間合併
        if tf == "D":
            return {"ok": False, "error": "全日盤只支援分鐘K（5K/15K/60K）"}
        parts, errs = [], []
        for s_ in ("regular", "afterhours"):
            d = _get_candles(tf, s_)
            if d.get("ok"):
                parts += d["candles"]
            else:
                errs.append(d.get("error"))
        if not parts and errs:
            return {"ok": False, "error": errs[0]}
        m = {b["t"]: b for b in parts}
        data = {"ok": True, "tf": tf, "session": sess, "candles": [m[t] for t in sorted(m)]}
    else:
        bars, err = _fetch_one(tf, sess)
        if bars is None:
            return {"ok": False, "error": err}
        data = {"ok": True, "tf": tf, "session": sess, "candles": bars}
    _candle_cache[key] = {"ts": time.time(), "data": data}
    return data


@app.route("/api/candles")
def candles():
    """台指期 K 線：session = regular 日盤 / afterhours 夜盤 / all 全日盤（兩者合併）"""
    tf = request.args.get("tf", "5")
    sess = request.args.get("session", "regular")
    if tf not in TF_DAYS or sess not in ("regular", "afterhours", "all"):
        return jsonify({"ok": False, "error": "參數錯誤"}), 400
    return jsonify(_get_candles(tf, sess))


WATCH_MAX = int(os.getenv("WATCH_MAX", "20"))   # 自選股數量上限
_watch_cache = {}
_wpool = ThreadPoolExecutor(max_workers=4)


def _is_stock_sym(sym):
    return bool(re.fullmatch(r"\d{4,6}[A-Z]?", sym)) or sym.startswith("IX")


def watch_one(sym):
    c = _watch_cache.get(sym)
    if c and time.time() - c[0] < 3:
        return c[1]
    try:
        if _is_stock_sym(sym):
            d = simplify(rest_stock.intraday.quote(symbol=sym))
        else:
            d = simplify(fcall(rest_fut.intraday.quote, symbol=sym))
        d["real"] = d.get("symbol")
        d["symbol"] = sym
        if not d.get("name"):
            d["name"] = sym
    except FugleAPIError as e:
        d = {"ok": False, "symbol": sym, "error": f"{e.status_code} {e.response_text}"}
    except Exception as e:  # noqa
        d = {"ok": False, "symbol": sym, "error": f"{type(e).__name__}: {e}"}
    _watch_cache[sym] = (time.time(), d)
    return d


@app.route("/api/watch")
def watch():
    """自選股報價：代碼以逗號分隔；數字代碼 / IX 開頭走股票行情，其餘走期權行情。"""
    syms = []
    for x in request.args.get("symbols", "").split(","):
        x = x.strip().upper()
        if re.fullmatch(r"[A-Z0-9*!]{2,16}", x) and x not in syms:
            syms.append(x)
    syms = syms[:WATCH_MAX]
    return jsonify({"ok": True, "items": list(_wpool.map(watch_one, syms)),
                    "serverTime": time.strftime("%H:%M:%S")})


@app.route("/api/book")
def book():
    """五檔：第一次查詢會訂閱 books 頻道，之後回傳 WebSocket 推送的最新委託簿。"""
    sym = request.args.get("symbol", "").strip().upper()
    ah = request.args.get("ah") == "1"
    if not re.fullmatch(r"[A-Z0-9]{3,16}", sym):
        return jsonify({"ok": False, "error": "商品代碼格式錯誤"}), 400
    ws_subscribe_books(sym, ah)
    b = _books.get(_fkey(sym, ah))
    if not b:
        return jsonify({"ok": True, "waiting": True, "connected": _ws_state["connected"], "error": _ws_state["error"]})
    return jsonify({"ok": True, "waiting": False, "symbol": sym, "age": round(time.time() - b["ts"], 1), **b})


@app.route("/api/debug")
def debug():
    """除錯：指數清單、選擇權合約解析結果與原始樣本。"""
    out = {"resolved_index": {k: v for k, v in _index_cache.items()}}
    for ex in ("TWSE", "TPEx"):
        out["indices_" + ex] = safe(lambda ex=ex: rest_stock.intraday.tickers(type="INDEX", exchange=ex))
    exps = safe(load_option_contracts)
    if isinstance(exps, dict) and "ok" not in exps:
        out["option_expiries"] = [
            {"id": k, "strikes": len(v["strikes"])} for k, v in exps.items()
        ]
    else:
        out["option_expiries"] = exps
    out["option_ticker_errors"] = _opt["errors"]
    with _ws_lock:
        out["ws"] = {**_ws_state, "subscribed": len(_ws_wanted), "books": len(_books_wanted)}
    with _flow_lock:
        out["flow_sample"] = dict(list(_flow.items())[:8])
    out["db"] = {"path": DB_PATH, **_db_state}
    out["txo_raw_sample"] = safe(
        lambda: fcall(rest_fut.intraday.tickers, type="OPTION", exchange="TAIFEX",
                      session="REGULAR", product="TXO")["data"][:5]
    )
    # 取第一個到期別、最接近價平的一組買/賣權，回傳原始報價
    try:
        e = next(iter(load_option_contracts().values()))
        center = center_price()
        ks = sorted(e["strikes"])
        k = min(ks, key=lambda x: abs(x - center)) if center else ks[len(ks) // 2]
        out["option_quote_sample"] = {
            "expiry": e["id"], "strike": k,
            "symbols": e["strikes"][k],
            "C_raw": safe(lambda: fcall(rest_fut.intraday.quote, symbol=e["strikes"][k].get("C"))),
            "P_raw": safe(lambda: fcall(rest_fut.intraday.quote, symbol=e["strikes"][k].get("P"))),
        }
    except Exception as ex:  # noqa
        out["option_quote_sample"] = f"{type(ex).__name__}: {ex}"
    return jsonify(out)


@app.route("/indicators/<path:name>")
def indicators_static(name):
    """內建指標 JS：放在 app.py 旁的 indicators 資料夾。"""
    return send_from_directory(os.path.join(BASE_DIR, "indicators"), name)


@app.route("/")
def index():
    for folder in (BASE_DIR, os.path.join(BASE_DIR, "static")):
        if os.path.exists(os.path.join(folder, "index.html")):
            return send_from_directory(folder, "index.html")
    return f"找不到 index.html，請放在 {BASE_DIR} 或其 static 子資料夾", 404


def flow_bootstrap():
    """登入後立刻訂閱近月期貨與最近幾個到期別的選擇權（價平附近），讓籌碼從開盤就開始累計。"""
    try:
        sym = pick_near_month_txf()
        ws_subscribe([sym], False)
        ws_subscribe([sym], True)
        exps = [e for e in selected_expiries() if not is_settled(e)][:FLOW_PREFETCH]
        ah = auto_night()                    # 預訂閱「現在這個時段」的選擇權；另一個時段在網頁切換過去時才訂閱
        center = center_price(ah)
        for e in exps:
            ks = sorted(e["strikes"])
            idx = min(range(len(ks)), key=lambda i: abs(ks[i] - center)) if center else len(ks) // 2
            span = OPTION_WINDOW + FLOW_EXTRA
            ws_subscribe([e["strikes"][k].get(cp) for k in ks[max(0, idx - span): idx + span + 1] for cp in ("C", "P")], ah)
            if ah:
                _mark_ah_since()
    except Exception as ex:  # noqa
        _ws_state["error"] = f"bootstrap: {type(ex).__name__}: {ex}"


# 啟動時就初始化資料庫並讀回今天已存的力道（不需要登入）
db_init()
_flow_load()


def _startup_env_login():
    time.sleep(1)
    if _session["logged_in"] or not env_login_ready():
        return
    _login_try["ts"] = time.time()
    try:
        login_from_env()
        print("[login] 已用 .env 登入")
    except Exception as e:  # noqa
        print(f"[login] .env 自動登入失敗：{e}")


if ENV_AUTO_LOGIN:
    if env_login_ready():
        threading.Thread(target=_startup_env_login, daemon=True).start()
    else:
        print("[login] ENV_AUTO_LOGIN=1 但 .env 設定不完整（需要 FUBON_ID / FUBON_PWD，以及 FUBON_CERT_PATH 或 FUBON_CERT_B64）")

if __name__ == "__main__":
    # 放到自己網站時：請用 HTTPS 反向代理（nginx / Caddy），並限制可存取的人；
    # 正式部署建議 gunicorn -w 1 --threads 8 app:app（只能 1 個 worker，因為登入狀態與力道累積都在記憶體）
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
