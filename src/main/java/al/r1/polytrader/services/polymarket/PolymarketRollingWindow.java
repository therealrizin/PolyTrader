package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.engine.ProbabilityTable;

import java.util.ArrayDeque;
import java.util.Deque;

public class PolymarketRollingWindow {

    private static final long WINDOW_MILLIS = 300_000L;

    private record TwapPoint(long observedAtMillis, double avg60) {
    }

    private final Deque<TwapPoint> window = new ArrayDeque<>();
    private long lastObservedAtMillis = -1;

    public synchronized void addAndUpdateTable(
            long observedAtMillis,
            double avg60,
            ProbabilityTable table) {

        if (table == null) {
            return;
        }

        if (observedAtMillis <= lastObservedAtMillis) {
            return;
        }

        if (Double.isNaN(avg60) || Double.isInfinite(avg60) || avg60 <= 0.0) {
            return;
        }

        lastObservedAtMillis = observedAtMillis;

        TwapPoint current = new TwapPoint(observedAtMillis, avg60);
        window.addLast(current);

        while (!window.isEmpty() && observedAtMillis - window.peekFirst().observedAtMillis() > WINDOW_MILLIS) {
            window.pollFirst();
        }

        table.updateNumberOfChecks();

        for (TwapPoint past : window) {
            if (past == current) {
                continue;
            }

            long elapsedMillis = observedAtMillis - past.observedAtMillis();
            long elapsedSeconds = elapsedMillis / 1000L;

            if (elapsedSeconds < 1 || elapsedSeconds > 300) {
                continue;
            }

            if (past.avg60() <= 0.0) {
                continue;
            }

            double changePure = avg60 / past.avg60();

            table.updateProbabilitiesTable((int) elapsedSeconds, changePure, false);
        }
    }
}