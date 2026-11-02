/* 內建指標共用函式與註冊表
 * 每個指標檔呼叫 TXI.register({ id, name, on, create(chart) -> { update(bars, ctx), destroy() } })
 *   bars: [{t,o,h,l,c,v}]；t 為「已加 8 小時」的 epoch 秒（分鐘K），或 'YYYY-MM-DD'（日K）
 *   ctx : { tf, sess, fetchBars(tf, sess) -> Promise<bars> }
 */
(function () {
  const TXI = (window.TXI = { defs: [] });
  TXI.register = d => TXI.defs.push(d);

  // Pine ta.ema：前 len 根以 SMA 當起點
  TXI.ema = (src, len) => {
    const out = new Array(src.length).fill(null), a = 2 / (len + 1);
    let n = 0, sum = 0, prev = null;
    for (let i = 0; i < src.length; i++) {
      const v = src[i];
      if (v == null) { out[i] = prev; continue; }
      if (prev == null) { n++; sum += v; if (n === len) { prev = sum / len; out[i] = prev; } }
      else { prev = a * v + (1 - a) * prev; out[i] = prev; }
    }
    return out;
  };
  // Pine ta.tr(true)
  TXI.tr = bars => bars.map((b, i) => i
    ? Math.max(b.h - b.l, Math.abs(b.h - bars[i - 1].c), Math.abs(b.l - bars[i - 1].c))
    : b.h - b.l);
  // 當日第幾分鐘（時間戳已加 8 小時，UTC 即台北時間）
  TXI.tod = t => Math.floor((((t % 86400) + 86400) % 86400) / 60);
  TXI.isDaily = bars => bars.length > 0 && typeof bars[0].t === "string";
  TXI.tfSeconds = tf => ({ "5": 300, "15": 900, "60": 3600 }[tf] || 0);
})();
