package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private final ProbabilityTable table;

    public TradingEngine(ProbabilityTable table) {
        this.table = table;
    }

    public UpDownEvEstimate estimateUpDown(
            BigDecimal currentPrice,
            BigDecimal referencePrice,
            int secondsLeft,
            double upMarketPrice,
            double downMarketPrice,
            double takerFee
    ) {
        double upChance = estimatedUpChance(currentPrice, referencePrice, secondsLeft);
        double downChance = 1.0 - upChance;

        double upEv = evForSide(upChance, upMarketPrice, takerFee);
        double downEv = evForSide(downChance, downMarketPrice, takerFee);

        MarketSide recommendedSide = upEv >= downEv ? MarketSide.UP : MarketSide.DOWN;
        double recommendedChance = recommendedSide == MarketSide.UP ? upChance : downChance;
        double recommendedEv = Math.max(upEv, downEv);

        return new UpDownEvEstimate(upChance, downChance, upEv, downEv, recommendedSide, recommendedChance, recommendedEv);
    }

    private double estimatedUpChance(BigDecimal currentPrice, BigDecimal referencePrice, int secondsLeft) {
        if (currentPrice == null || referencePrice == null || referencePrice.signum() == 0) {
            return 0.5;
        }

        double requiredPctChange = referencePrice.subtract(currentPrice)
                .divide(currentPrice, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();

        if (requiredPctChange <= 0) {
            return 1.0 - table.getChance(secondsLeft, requiredPctChange);
        } else {
            return table.getChance(secondsLeft, requiredPctChange);
        }
    }

    private double evForSide(double winChance, double marketPrice, double takerFee) {
        if (marketPrice <= 0.0 || marketPrice >= 1.0) {
            return Double.NEGATIVE_INFINITY;
        }
        double grossPayout = 1.0 / marketPrice;
        double netPayout = 1.0 + (grossPayout - 1.0) * (1.0 - takerFee);
        return winChance * netPayout - 1.0;
    }
}