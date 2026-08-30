// services/model/PriceTickAggregators.java
package al.r1.polytrader.services.model;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Getter
@Component
public class PriceTickAggregators {
    private final TickAggregator binance = new TickAggregator();
    private final TickAggregator coinbase = new TickAggregator();
    private final TickAggregator kraken = new TickAggregator();
    private final TickAggregator bybit = new TickAggregator();
    private final TickAggregator okx = new TickAggregator();
}