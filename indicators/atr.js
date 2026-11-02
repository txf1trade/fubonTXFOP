/* ATR Trend（主圖）—— 移植自 Pine「ATR Trend」
 * 注意：原 Pine 的 get_atr(5, …) 實際寫死用 5 期 EMA，「期數」(14) 只用在最高/最低點，這裡照原邏輯。
 */
TXI.register({
  id: "atr", name: "ATR Trend", on: true,
  defaults: { len: 14, atrLen: 5, mult: 2.0, colorCoding: true },
  create(chart) {
    const P = Object.assign({}, this.defaults);
    const s = chart.addLineSeries({ lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    return {
      update(bars) {
        const n = bars.length;
        if (!n) { s.setData([]); return; }
        const atr = TXI.ema(TXI.tr(bars), P.atrLen).map(v => v == null ? null : v * P.mult);
        const upper = new Array(n).fill(null), lower = new Array(n).fill(null);
        for (let i = P.len - 1; i < n; i++) {
          let hi = -Infinity, lo = Infinity;
          for (let j = i - P.len + 1; j <= i; j++) { hi = Math.max(hi, bars[j].h); lo = Math.min(lo, bars[j].l); }
          upper[i] = hi; lower[i] = lo;
        }
        const out = [];
        let prev = null, ud = null;
        for (let i = 0; i < n; i++) {
          const b = bars[i];
          const dir = i && upper[i] != null && upper[i - 1] != null && upper[i] > upper[i - 1] ? "up"
            : i && lower[i] != null && lower[i - 1] != null && lower[i] < lower[i - 1] ? "down" : "neutral";
          let t = null;
          if (dir === "up") { t = atr[i] == null ? null : b.l - atr[i]; if (t != null && prev != null && t < prev) t = prev; }
          else if (dir === "down") { t = atr[i] == null ? null : b.h + atr[i]; if (t != null && prev != null && t > prev) t = prev; }
          else t = prev;
          if (t != null && prev != null) { if (t > prev) ud = "up"; else if (t < prev) ud = "down"; }
          prev = t;
          if (t == null) { out.push({ time: b.t }); continue; }
          const half = b.o + (b.c - b.o) / 2;
          const color = P.colorCoding
            ? (half < t && ud !== "down" ? "#ff4100" : ud === "up" ? "#00cbff" : half > t ? "#ff4101" : "#ff0099")
            : (ud === "up" ? "#00cbff" : "#ff0099");
          out.push({ time: b.t, value: t, color });
        }
        s.setData(out);
      },
      destroy() { chart.removeSeries(s); },
    };
  },
});
