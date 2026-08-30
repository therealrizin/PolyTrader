// services/model/SecondTickAggregator.java
package al.r1.polytrader.services.model;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TickAggregator {

    private final AtomicReference<BigDecimal> sum = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicInteger count = new AtomicInteger(0);
    private volatile BigDecimal lastPrice;

    public void record(BigDecimal price) {
        if (price == null) return;
        lastPrice = price;
        sum.updateAndGet(current -> current.add(price));
        count.incrementAndGet();
    }

    public synchronized FlushResult flush() {
        BigDecimal total = sum.getAndSet(BigDecimal.ZERO);
        int n = count.getAndSet(0);
        if (n == 0) return new FlushResult(lastPrice, 0);
        return new FlushResult(total.divide(BigDecimal.valueOf(n), 2, RoundingMode.HALF_UP), n);
    }

    public record FlushResult(BigDecimal averagePrice, int tickCount) {}
}