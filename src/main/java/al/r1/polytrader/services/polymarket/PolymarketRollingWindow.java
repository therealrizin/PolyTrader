package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.engine.ProbabilityTable;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Feeds the shared ProbabilityTable from Polymarket's own Chainlink-computed
 * 60s TWAP samples, rather than recomputing an average locally.
 *
 * Unlike RollingPriceWindow (Binance), this does NOT sum/average raw ticks —
 * Chainlink already hands us the 60s TWAP value directly, and we don't
 * reproduce their smoothing/sampling logic locally (see Polymarket docs:
 * "do not independently reproduce the value without a specification from
 * Chainlink").
 *
 * Ordering guarantee: elapsedSeconds is derived only from Chainlink's
 * observationsTimestamp (payload.timestamp), never wall-clock arrival time.
 * A sample at or before the last accepted observation is dropped rather than
 * risked corrupting the table with an out-of-order or duplicate RTDS push
 * (e.g. after a reconnect).
 */
public class PolymarketRollingWindow {

    private record TwapPoint(long observedAtMillis, double avg60) {}

    private final Deque<TwapPoint> window = new ArrayDeque<>();
    private long lastObservedAtMillis = -1;

    public synchronized void addAndUpdateTable(long observedAtMillis, double avg60, ProbabilityTable table) {
        if (observedAtMillis <= lastObservedAtMillis) {
            return; // stale, duplicate, or out-of-order RTDS delivery — skip
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