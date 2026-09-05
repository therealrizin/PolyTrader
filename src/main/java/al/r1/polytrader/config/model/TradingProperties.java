package al.r1.polytrader.config.model;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "trading")
public record TradingProperties(
        boolean mock,
        double minimumExpectedEv,
        double minimumWinChance,
        double takerFee,
        BigDecimal betAmount,
        int minimumSecondsSinceOpen,
        double sellEvMultiplier
) {}