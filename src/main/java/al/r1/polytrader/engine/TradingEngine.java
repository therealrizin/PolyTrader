package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.Side;
import al.r1.polytrader.services.model.Prices;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private Prices prices;
    private ProbabilityTable table;

    public TradingEngine(Prices prices, ProbabilityTable table) {
        this.prices = prices;
        this.table = table;
    }

    public EvEstimate estimatedEv(
            BigDecimal targetPrice,
            Integer secondsLeft,
            Double marketValue
    ) {
        double chance = estimatedChance(targetPrice, secondsLeft);

        if (marketValue == null || marketValue <= 0.0 || marketValue >= 1.0) {
            throw new IllegalArgumentException("Market value must be between 0 and 1");
        }

        // STAY
        double stayChance = 1.0 - chance;
        double stayPayout = 1.0 / (1.0 - marketValue);
        double stayEv = stayChance * stayPayout - 1.0;

        // CHANGE
        double changeChance = chance;
        double changePayout = 1.0 / marketValue;
        double changeEv = changeChance * changePayout - 1.0;

        Side recommendedSide = stayEv >= changeEv
                ? Side.STAY
                : Side.CHANGE;

        return new EvEstimate(
                stayEv,
                changeEv,
                chance,
                recommendedSide
        );
    }

    public Double estimatedChance(BigDecimal targetPrice, Integer secondsLeft) {
        BigDecimal adjustedPrice = prices.getAvg60sPrice()
                .divide(prices.getPolymarketPrice(), 2, RoundingMode.HALF_UP)
                .multiply(prices.getAvgPrice());

        Double requiredPercentageChange = adjustedPrice.divide(targetPrice, 3, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .subtract(BigDecimal.valueOf(100))
                .doubleValue();

        return table.getChance(secondsLeft, requiredPercentageChange);
    }
}
