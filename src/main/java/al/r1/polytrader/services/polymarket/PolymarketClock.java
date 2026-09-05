package al.r1.polytrader.services.polymarket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class PolymarketClock {

    private final AtomicLong offsetMillis = new AtomicLong(0);
    private volatile boolean hasSample = false;

    public void recordServerTimestamp(long serverEpochMillis) {
        long localNow = System.currentTimeMillis();
        long newOffset = serverEpochMillis - localNow;

        long previousOffset = offsetMillis.getAndSet(newOffset);
        boolean first = !hasSample;
        hasSample = true;

        if (first || Math.abs(newOffset - previousOffset) > 500) {
            log.info("Polymarket clock offset updated: {}ms -> {}ms (local clock is {} Polymarket's)",
                    previousOffset, newOffset,
                    newOffset >= 0 ? "behind" : "ahead of");
        }
    }

    public long nowMillis() {
        return System.currentTimeMillis() + offsetMillis.get();
    }
}