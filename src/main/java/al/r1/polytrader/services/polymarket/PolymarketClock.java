package al.r1.polytrader.services.polymarket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the offset between our local system clock and Polymarket's own
 * clock, so time-remaining calculations for a market window are anchored to
 * Polymarket's notion of "now" rather than our local clock (which can drift
 * or simply be wrong relative to whatever server actually resolves the
 * window).
 *
 * Fed from two sources, whichever is freshest:
 *  - The `timestamp` field on RTDS TWAP messages (PolymarketTwapClient),
 *    which is Polymarket's own observation time for that price tick.
 *  - The HTTP `Date` response header on Gamma REST calls
 *    (PolymarketDataProvider), as a fallback/cross-check since RTDS and
 *    Gamma could in principle be served by different clocks.
 *
 * This is a best-effort offset, not a full round-trip-corrected NTP sync —
 * but it's meaningfully better than trusting our local clock unconditionally,
 * which was the previous behavior.
 */
@Slf4j
@Component
public class PolymarketClock {

    private final AtomicLong offsetMillis = new AtomicLong(0);
    private volatile boolean hasSample = false;

    /**
     * @param serverEpochMillis a timestamp Polymarket itself reported for
     *                          "now" (an HTTP Date header, or an RTDS
     *                          payload timestamp)
     */
    public void recordServerTimestamp(long serverEpochMillis) {
        long localNow = System.currentTimeMillis();
        long newOffset = serverEpochMillis - localNow;

        long previousOffset = offsetMillis.getAndSet(newOffset);
        boolean first = !hasSample;
        hasSample = true;

        // Only log on the first sample or a meaningful jump — otherwise
        // this fires every second and drowns out everything else.
        if (first || Math.abs(newOffset - previousOffset) > 500) {
            log.info("Polymarket clock offset updated: {}ms -> {}ms (local clock is {} Polymarket's)",
                    previousOffset, newOffset,
                    newOffset >= 0 ? "behind" : "ahead of");
        }
    }

    /**
     * Best estimate of Polymarket's current time. Falls back to the local
     * clock (offset 0) until at least one sample has been recorded.
     */
    public long nowMillis() {
        return System.currentTimeMillis() + offsetMillis.get();
    }

    public boolean hasSample() {
        return hasSample;
    }

    public long getOffsetMillis() {
        return offsetMillis.get();
    }
}