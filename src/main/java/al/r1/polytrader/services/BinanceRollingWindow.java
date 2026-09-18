package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;

public class BinanceRollingWindow {

    private static final long AVG_WINDOW_MILLIS = 60_000L;
    private static final long HISTORY_WINDOW_MILLIS = 300_000L;

    private record PricePoint(long second, double close, double avg60) {}

    private final Deque<PricePoint> points = new ArrayDeque<>();
    private final Deque<Integer> ticks = new ArrayDeque<>(3);
    private long currentSecond = -1;
    private BigDecimal currentClose;
    private Double previousClose;

    public synchronized Optional<BigDecimal> record(long observedAtMillis, BigDecimal price, ProbabilityTable table) {
        if (price == null || price.signum() <= 0 || observedAtMillis <= 0 || table == null) {
            return Optional.empty();
        }

        long second = observedAtMillis / 1_000L * 1_000L;
        if (currentSecond < 0) {
            currentSecond = second;
            currentClose = price;
            return Optional.empty();
        }
        if (second < currentSecond) {
            return Optional.empty();
        }
        if (second == currentSecond) {
            currentClose = price;
            return Optional.empty();
        }

        BigDecimal completedClose = currentClose;
        updateTable(currentSecond, completedClose.doubleValue(), table);
        currentSecond = second;
        currentClose = price;
        return Optional.of(completedClose);
    }

    private void updateTable(long second, double close, ProbabilityTable table) {
        points.addLast(new PricePoint(second, close, 0.0));
        while (!points.isEmpty() && second - points.peekFirst().second() > HISTORY_WINDOW_MILLIS) {
            points.pollFirst();
        }

        double avg60 = points.stream()
                .filter(point -> second - point.second() < AVG_WINDOW_MILLIS)
                .mapToDouble(PricePoint::close)
                .average()
                .orElse(close);
        points.pollLast();
        PricePoint current = new PricePoint(second, close, avg60);
        points.addLast(current);

        if (previousClose != null) {
            ticks.addLast(Double.compare(close, previousClose));
            while (ticks.size() > 3) ticks.pollFirst();
        }
        previousClose = close;
        if (ticks.size() < 3) return;

        int trendLayer = ProbabilityTable.trendLayer(
                ticks.peekFirst(), ticks.stream().skip(1).findFirst().orElse(0), ticks.peekLast());
        table.updateNumberOfChecks(trendLayer);

        for (PricePoint past : points) {
            if (past == current) continue;
            long elapsedSeconds = (second - past.second()) / 1_000L;
            if (elapsedSeconds < 1 || elapsedSeconds > 300) continue;
            table.updateProbabilitiesTable((int) elapsedSeconds, avg60 / past.avg60(), false, trendLayer);
        }
    }
}
