package al.r1.polytrader.config.trading;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;

@ConfigurationProperties(prefix = "trading")
public record TradingProperties(
        boolean mock,
        double minimumExpectedEv,
        double minimumWinChance,
        double takerFee,
        BigDecimal mockBetAmount
) {
    public TradingProperties {
        if (mockBetAmount == null) {
            mockBetAmount = BigDecimal.ONE;
        }
    }
}