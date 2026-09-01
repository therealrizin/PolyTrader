package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.Side;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
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

    /**
     * Evaluates a Polymarket "up or down since window open" binary market.
     * {@code referencePrice} is the price observed at the window's open
     * (the value the market resolves against); {@code currentPrice} is our
     * live blended feed's current reading.
     *
     * The historical probability table is cumulative-toward-zero (see
     * ProbabilityTable), so {@code table.getChance(seconds, x)} for a
     * positive x approximates P(price has risen by at least x% within
     * `seconds`), and for negative x approximates P(price has fallen by at
     * least |x|% within `seconds`). We use that to derive P(Up) from
     * however far price already sits from the reference:
     *  - if currently at/above reference: P(Up) = 1 - P(falls back below)
     *  - if currently below reference:    P(Up) = P(rises back above)
     */
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
            return 0.5; // no information yet, treat as a coin flip
        }

        double requiredPctChange = referencePrice.subtract(currentPrice)
                .divide(currentPrice, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();

        if (requiredPctChange <= 0) {
            // Already at/above the reference price: Up survives unless
            // price falls back below the reference by at least the gap.
            return 1.0 - table.getChance(secondsLeft, requiredPctChange);
        } else {
            // Below the reference price: Up needs a rise of at least
            // requiredPctChange to cross back above it.
            return table.getChance(secondsLeft, requiredPctChange);
        }
    }

    /**
     * EV per $1 staked. {@code takerFee} is modeled as a cut of the
     * *profit* only (not the stake) — this is a placeholder assumption,
     * flagged elsewhere as needing verification against Polymarket's actual
     * fee schedule.
     */
    private double evForSide(double winChance, double marketPrice, double takerFee) {
        if (marketPrice <= 0.0 || marketPrice >= 1.0) {
            return Double.NEGATIVE_INFINITY;
        }
        double grossPayout = 1.0 / marketPrice;
        double netPayout = 1.0 + (grossPayout - 1.0) * (1.0 - takerFee);
        return winChance * netPayout - 1.0;
    }
}