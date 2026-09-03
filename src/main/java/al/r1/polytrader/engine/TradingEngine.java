package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.MarketSide;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private static final int TWAP_WINDOW_SECONDS = 60;
    private static final double ZERO_MOVEMENT_THRESHOLD_PERCENT = 0.0005;

    private final ProbabilityTable table;

    public TradingEngine(ProbabilityTable table) {
        this.table = table;
    }

    public EvEstimate estimateUpDown(
            BigDecimal currentLivePrice,
            BigDecimal currentTwapPrice,
            BigDecimal referencePrice,
            int secondsLeft,
            double upMarketPrice,
            double downMarketPrice,
            double takerFee
    ) {
        double upChance = estimatedUpChance(
                currentLivePrice,
                currentTwapPrice,
                referencePrice,
                secondsLeft
        );

        double downChance = 1.0 - upChance;

        double upEv = evForBuySide(
                upChance,
                upMarketPrice,
                takerFee
        );

        double downEv = evForBuySide(
                downChance,
                downMarketPrice,
                takerFee
        );

        MarketSide recommendedSide = upEv >= downEv
                ? MarketSide.UP
                : MarketSide.DOWN;

        double recommendedChance = recommendedSide == MarketSide.UP
                ? upChance
                : downChance;

        double recommendedEv = Math.max(upEv, downEv);

        return new EvEstimate(
                upChance,
                downChance,
                upEv,
                downEv,
                recommendedSide,
                recommendedChance,
                recommendedEv
        );
    }

    /**
     * Returns the maximum price at which buying this side still produces
     * at least the requested EV.
     *
     * Existing EV formula:
     *
     * grossPayout = 1 / price
     * netPayout   = 1 + (grossPayout - 1) * (1 - fee)
     * EV          = winChance * netPayout - 1
     *
     * Solving EV >= targetEv for price gives:
     *
     * price <= (1 - fee)
     *          / (((1 + targetEv) / winChance) - fee)
     *
     * Example:
     *
     * winChance = 0.63
     * targetEv  = 0.005
     * fee       = 0.07
     *
     * returns the highest price we should accept.
     */
    public double maxBuyPriceForEv(
            double winChance,
            double targetEv,
            double takerFee
    ) {
        if (Double.isNaN(winChance)
                || Double.isInfinite(winChance)
                || winChance <= 0.0
                || winChance > 1.0) {
            return 0.0;
        }

        if (Double.isNaN(targetEv)
                || Double.isInfinite(targetEv)) {
            return 0.0;
        }

        if (Double.isNaN(takerFee)
                || Double.isInfinite(takerFee)
                || takerFee < 0.0
                || takerFee >= 1.0) {
            return 0.0;
        }

        double denominator =
                ((1.0 + targetEv) / winChance) - takerFee;

        if (denominator <= 0.0) {
            return 0.0;
        }

        double maxPrice =
                (1.0 - takerFee) / denominator;

        if (Double.isNaN(maxPrice)
                || Double.isInfinite(maxPrice)) {
            return 0.0;
        }

        return Math.clamp(maxPrice, 0.0, 1.0);
    }

    public double evForBuySide(
            double winChance,
            double marketPrice,
            double takerFee
    ) {
        if (marketPrice <= 0.0 || marketPrice >= 1.0) {
            return Double.NEGATIVE_INFINITY;
        }

        double grossPayout = 1.0 / marketPrice;

        double netPayout =
                1.0 + (grossPayout - 1.0) * (1.0 - takerFee);

        return winChance * netPayout - 1.0;
    }

    private double estimatedUpChance(
            BigDecimal currentLivePrice,
            BigDecimal currentTwapPrice,
            BigDecimal referencePrice,
            int secondsLeft
    ) {
        if (currentLivePrice == null
                || currentTwapPrice == null
                || referencePrice == null
                || currentLivePrice.signum() <= 0
                || currentTwapPrice.signum() <= 0) {
            return 0.5;
        }

        secondsLeft = Math.clamp(secondsLeft, 0, 300);

        if (secondsLeft >= TWAP_WINDOW_SECONDS) {
            double requiredPctChange =
                    percentageChange(currentLivePrice, referencePrice);

            return probabilityOfReaching(
                    requiredPctChange,
                    secondsLeft
            );
        }

        double futureWeight =
                (double) secondsLeft / TWAP_WINDOW_SECONDS;

        if (futureWeight <= 0.0) {
            return currentTwapPrice.compareTo(referencePrice) > 0
                    ? 1.0
                    : 0.0;
        }

        double currentTwap = currentTwapPrice.doubleValue();
        double reference = referencePrice.doubleValue();

        double requiredFutureLivePrice =
                (reference - currentTwap * (1.0 - futureWeight))
                        / futureWeight;

        double currentLive = currentLivePrice.doubleValue();

        double requiredPctChange =
                ((requiredFutureLivePrice - currentLive) / currentLive)
                        * 100.0;

        return probabilityOfReaching(
                requiredPctChange,
                secondsLeft
        );
    }

    private double probabilityOfReaching(
            double requiredPctChange,
            int secondsLeft
    ) {
        secondsLeft = Math.clamp(secondsLeft, 0, 300);

        if (Math.abs(requiredPctChange) < ZERO_MOVEMENT_THRESHOLD_PERCENT) {
            return 0.5;
        }

        if (requiredPctChange < 0.0) {
            double probabilityDown =
                    table.getChance(
                            secondsLeft,
                            Math.abs(requiredPctChange)
                    );

            return clampProbability(1.0 - probabilityDown);
        }

        double probabilityUp =
                table.getChance(
                        secondsLeft,
                        requiredPctChange
                );

        return clampProbability(probabilityUp);
    }

    private double percentageChange(
            BigDecimal from,
            BigDecimal to
    ) {
        return to.subtract(from)
                .divide(from, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();
    }

    public double holdValuePerShare(double winChance) {
        return clampProbability(winChance);
    }

    public double netSellValuePerShare(
            double currentBid,
            double takerFee
    ) {
        if (currentBid <= 0.0 || currentBid >= 1.0) {
            return 0.0;
        }

        double fee =
                takerFee
                        * currentBid
                        * (1.0 - currentBid);

        return currentBid - fee;
    }

    private double clampProbability(double probability) {
        if (Double.isNaN(probability)
                || Double.isInfinite(probability)) {
            return 0.5;
        }

        return Math.clamp(probability, 0.0, 1.0);
    }
}