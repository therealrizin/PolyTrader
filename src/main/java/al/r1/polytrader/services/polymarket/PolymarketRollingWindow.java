package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.engine.ProbabilityTable;

import java.util.ArrayDeque;
import java.util.Deque;

public class PolymarketRollingWindow {

    private record TwapPoint(long observedAtMillis, double avg60) {}

    private final Deque<TwapPoint> window = new ArrayDeque<>();
    private long lastObservedAtMillis = -1;

    public synchronized void addAndUpdateTable(long observedAtMillis, double avg60, ProbabilityTable table) {
        if (observedAtMillis <= lastObservedAtMillis) {
            return;
        }
        lastObservedAtMillis = observedAtMillis;

        window.addLast(new TwapPoint(observedAtMillis, avg60));
        window.removeIf(p -> observedAtMillis - p.observedAtMillis() > 300_000);

        table.updateNumberOfChecks();
        for (TwapPoint past : window) {
            if (past.observedAtMillis() == observedAtMillis) continue;
            long elapsedSeconds = (observedAtMillis - past.observedAtMillis()) / 1000;
            if (elapsedSeconds < 1 || elapsedSeconds > 300) continue;

            double changePure = avg60 / past.avg60();
            table.updateProbabilitiesTable((int) elapsedSeconds, changePure, false);
        }
    }
}