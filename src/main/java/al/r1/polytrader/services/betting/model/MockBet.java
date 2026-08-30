package al.r1.polytrader.services.betting.model;

import al.r1.polytrader.engine.model.MarketSide;

import java.math.BigDecimal;
import java.time.Instant;

public record MockBet(
        String id,
        String marketSlug,
        MarketSide side,
        BigDecimal amount,
        BigDecimal priceBetAt,
        BigDecimal priceToAchieve,
        BigDecimal marketPriceAtBet,
        double countedEv,
        double countedWinChance,
        BigDecimal potentialValue,
        Instant placedAt,
        Instant resolvesAt,
        BetStatus status,
        BigDecimal priceAtResolution,
        BigDecimal profitLoss
) {}