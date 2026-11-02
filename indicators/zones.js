/* 多空目標（台指期區間必來點）—— 移植自 Pine「台指期區間必來點」
 * 每個交易時段 = 預期振幅(長期加權 / 短期平均 / 昨日振幅 的加權) ，以前收（或今開）為多空線，
 * 向上/向下畫 目標 與 區間頂/底。
 * 「全日盤」：夜盤(15:00~05:00) + 日盤(08:45~13:45) 視為一個交易日，需要另外抓另一個時段的K線合併。
 * 僅支援分鐘K（5K/15K/60K）。
 */
TXI.register({
  id: "zones", name: "多空目標", on: false,
  defaults: { mode: "全日盤", lookback: 20, shortN: 5, base: "前收", wLong: 0.5, wShort: 0.3, wLast: 0.2,
              linear: true, kTarget: 0.25, kRange: 0.5, showSessions: 3 },
  create(chart) {
    const P = Object.assign({}, this.defaults);
    const LV = [   // [名稱, 係數, 顏色, 線寬]
      ["區間頂", () => P.kRange, "#ff3b3b", 2], ["多方目標", () => P.kTarget, "rgba(255,59,59,.65)", 1],
      ["多空線", () => 0, "#00b0ff", 2],
      ["空方目標", () => -P.kTarget, "rgba(0,192,118,.65)", 1], ["區間底", () => -P.kRange, "#00c076", 2],
    ];
    const pool = [];   // pool[k][j]：往前第 k 個時段、第 j 條線
    for (let k = 0; k < P.showSessions; k++) {
      pool.push(LV.map(([name, , color, w]) => chart.addLineSeries({
        color, lineWidth: w, priceLineVisible: false, crosshairMarkerVisible: false,
        lastValueVisible: k === 0, title: k === 0 ? name : "" })));
    }
    const clear = () => pool.forEach(r => r.forEach(s => s.setData([])));
    let tok = 0;

    const avg = (a, n) => { const s = Math.min(n, a.length); let t = 0; for (let i = 0; i < s; i++) t += a[a.length - 1 - i]; return s ? t / s : null; };
    const wma = (a, n, lin) => {
      const s = Math.min(n, a.length); let num = 0, den = 0;
      for (let i = 0; i < s; i++) { const w = lin ? s - i : 1; num += w * a[a.length - 1 - i]; den += w; }
      return s ? num / den : null;
    };
    const expected = a => {
      if (!a.length) return null;
      const lv = wma(a, P.lookback, P.linear), sv = avg(a, P.shortN), last = a[a.length - 1];
      const ws = P.wLong + P.wShort + P.wLast;
      return ws > 0 ? (P.wLong * lv + P.wShort * sv + P.wLast * last) / ws : lv;
    };

    return {
      async update(bars, ctx) {
        const my = ++tok;
        if (!bars.length || TXI.isDaily(bars)) { clear(); return; }
        let all = bars;
        if (P.mode === "全日盤" && ctx.sess !== "all") {
          try {
            const ob = await ctx.fetchBars(ctx.tf, ctx.sess === "regular" ? "afterhours" : "regular");
            if (my !== tok) return;
            const m = new Map();
            for (const b of ob) m.set(b.t, b);
            for (const b of bars) m.set(b.t, b);
            all = [...m.values()].sort((a, b) => a.t - b.t);
          } catch (e) { /* 抓不到另一個時段就只用目前這個時段 */ }
        }
        if (my !== tok) return;

        const ranges = [], sess = [];
        let cur = null, last = null, lastDayClose = null, prevClose = null;
        for (const b of all) {
          const m = TXI.tod(b.t), inDay = m >= 525 && m <= 825, inNight = m >= 900 || m <= 300;
          const inS = P.mode === "日盤" ? inDay : (inDay || inNight);
          if (inS) {
            const isNew = last == null ? true
              : P.mode === "日盤" ? m < last
              : (m >= 900 && last < 900) || (last > 300 && last <= 825 && m < last && m > 300);
            if (isNew) {
              if (cur) ranges.push(cur.H - cur.L);
              cur = { t: [], H: b.h, L: b.l, base: P.base === "前收" ? (lastDayClose ?? prevClose) : b.o, exp: expected(ranges) };
              sess.push(cur);
            } else { cur.H = Math.max(cur.H, b.h); cur.L = Math.min(cur.L, b.l); }
            cur.t.push(b.t);
            last = m;
          }
          if (inDay) lastDayClose = b.c;
          prevClose = b.c;
        }

        const shown = new Set(bars.map(b => b.t));   // 只畫在目前圖上有K棒的時間，避免撐出多餘的時間格
        clear();
        // 右側標籤放在「目前圖上有K棒的最新時段」（例如盤後夜盤已開始、但日盤圖上還沒有K棒時，標籤留在前一時段）
        let lead = -1;
        for (let k = 0; k < P.showSessions && k < sess.length; k++) {
          const s = sess[sess.length - 1 - k];
          if (s.base == null || s.exp == null) continue;
          const ts = s.t.filter(t => shown.has(t));
          if (!ts.length) continue;
          if (lead < 0) lead = k;
          LV.forEach(([, f], j) => pool[k][j].setData(ts.map(t => ({ time: t, value: s.base + f() * s.exp }))));
        }
        pool.forEach((row, k) => row.forEach((sr, j) => sr.applyOptions({ lastValueVisible: k === lead, title: k === lead ? LV[j][0] : "" })));
      },
      destroy() { tok++; pool.forEach(r => r.forEach(s => chart.removeSeries(s))); },
    };
  },
});
