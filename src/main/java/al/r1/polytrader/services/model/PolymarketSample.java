package al.r1.polytrader.services.model;

import java.math.BigDecimal;

public record PolymarketSample(long timestampMillis, BigDecimal polymarketPrice, BigDecimal avgPrice) {
}
