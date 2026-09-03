package al.r1.polytrader.services.polymarket.model;

import java.math.BigDecimal;

public record PolymarketMarketSnapshot(
        String slug,
        BigDecimal upPrice,
        BigDecimal downPrice,
        BigDecimal upBid,
        BigDecimal downBid,
        long secondsUntilClose,
        BigDecimal strikePriceUsd,
        long secondsSinceOpen
) {}