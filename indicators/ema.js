/* EMA 均線（僅 15K 週期顯示）
 *   EMA76  「日線」：往上(比前一根高)紅、否則綠，線寬 3
 *   EMA380 「週線」：藍色，線寬 3
 * 兩條各自在「指標 ▾」選單中勾選開關。
 */
(function () {
  const make = (id, name, len, colorFn, color) => TXI.register({
    id, name, on: true,
    create(chart) {
      const s = chart.addLineSeries({ color, lineWidth: 3, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
      return {
        update(bars, ctx) {
          if (ctx.tf !== "15" || !bars.length) { s.setData([]); return; }
          const e = TXI.ema(bars.map(b => b.c), len);
          s.setData(bars.map((b, i) => e[i] == null ? { time: b.t }
            : { time: b.t, value: e[i], color: colorFn(e[i], i ? e[i - 1] : null) }));
        },
        destroy() { chart.removeSeries(s); },
      };
    },
  });
  make("ema76", "EMA76 日線", 76, (v, p) => (p != null && v > p ? "#f52525" : "#15af1a"), "#15af1a");
  make("ema380", "EMA380 週線", 380, () => "#2186c0", "#2186c0");
})();
