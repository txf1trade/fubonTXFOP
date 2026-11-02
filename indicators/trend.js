/* 台指順勢波段（圖表下方色帶）—— 移植自 Pine「台指順勢波段」
 * 15 分K 收盤連續 N 根在 EMA(76) 之上＝多（紅），連續 N 根在其下＝空（綠），其餘維持前一個狀態。
 * 與原 Pine 的差異：EMA 尚未算出來（前 75 根15分K）時不顯示。僅支援分鐘K。
 */
TXI.register({
  id: "trend", name: "順勢波段", on: false, bottomBand: true,
  defaults: { emaLen: 76, confirm: 2 },
  create(chart) {
    const P = Object.assign({}, this.defaults);
    const s = chart.addHistogramSeries({
      priceScaleId: "trend", priceLineVisible: false, lastValueVisible: false,
      priceFormat: { type: "custom", formatter: v => v > 0 ? "多" : v < 0 ? "空" : "" },
      autoscaleInfoProvider: () => ({ priceRange: { minValue: -1, maxValue: 1 } }),
    });
    chart.priceScale("trend").applyOptions({ scaleMargins: { top: 0.76, bottom: 0.2 }, visible: false });
    let tok = 0;
    return {
      async update(bars, ctx) {
        const my = ++tok;
        const dur = TXI.tfSeconds(ctx.tf);
        if (!bars.length || TXI.isDaily(bars) || !dur) { s.setData([]); return; }
        let b15 = bars;
        if (ctx.tf !== "15") {
          try { b15 = await ctx.fetchBars("15", ctx.sess); } catch (e) { s.setData([]); return; }
          if (my !== tok) return;
        }
        const ema = TXI.ema(b15.map(b => b.c), P.emaLen);
        const st = new Array(b15.length).fill(null);
        let pv = null;
        for (let i = 0; i < b15.length; i++) {
          if (i >= P.confirm - 1) {
            let bull = true, bear = true;
            for (let j = 0; j < P.confirm; j++) {
              const e = ema[i - j], c = b15[i - j].c;
              if (e == null) { bull = bear = false; break; }
              if (c <= e) bull = false;
              if (c >= e) bear = false;
            }
            if (bull) pv = 1; else if (bear) pv = -1;
          }
          st[i] = pv;
        }
        // 對應到目前圖上的K棒：取「收盤時間 <= 該K棒收盤時間」的最後一根15分K（不偷看未來）
        const out = [];
        let p = -1;
        for (const b of bars) {
          while (p + 1 < b15.length && b15[p + 1].t + 900 <= b.t + dur) p++;
          const v = p >= 0 ? st[p] : null;
          out.push(v == null ? { time: b.t }
            : { time: b.t, value: v, color: v > 0 ? "rgba(246,70,93,.85)" : "rgba(0,192,118,.85)" });
        }
        s.setData(out);
      },
      destroy() { tok++; chart.removeSeries(s); },
    };
  },
});
