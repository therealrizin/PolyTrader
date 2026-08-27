package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.binance.model.BinanceKline;

import java.util.ArrayDeque;
import java.util.Deque;

public class RollingPriceWindow {

    private record PricePoint(long closeTime, double close, double avg60) {}

    private final Deque<PricePoint> window = new ArrayDeque<>();
    private double windowSum60 = 0;

    public void addAndUpdateTable(BinanceKline kline, ProbabilityTable table) {
        double close = kline.close().doubleValue();
        long closeTime = kline.closeTime();

        windowSum60 += close;
        window.addLast(new PricePoint(closeTime, close, 0)); // avg computed below

        // trim 60s window for the rolling average
        while (!window.isEmpty() && closeTime - window.peekFirst().closeTime() >= 60_000) {
            windowSum60 -= window.peekFirst().close();
            window.pollFirst();
        }
        double currentAvg60 = windowSum60 / window.size();

        // trim anything older than 300s — no longer needed for lookback
        window.removeIf(p -> closeTime - p.closeTime() > 300_000);

        // compare current avg against every prior point still in the 300s window
        for (PricePoint past : window) {
            if (past.closeTime() == closeTime) continue;
            long elapsedSeconds = (closeTime - past.closeTime()) / 1000;
            if (elapsedSeconds < 1 || elapsedSeconds > 300) continue;

            double changePure = currentAvg60 / past.avg60();
            table.updateProbabilitiesTable((int) elapsedSeconds, changePure, true);
        }
    }
}