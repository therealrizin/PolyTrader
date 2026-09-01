package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private static final int TWAP_WINDOW_SECONDS = 60;

    private final ProbabilityTable table;

    public TradingEngine(ProbabilityTable table) {
        this.table = table;
    }

    public UpDownEvEstimate estimateUpDown(
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

        double upEv = evForSide(upChance, upMarketPrice, takerFee);
        double downEv = evForSide(downChance, downMarketPrice, takerFee);

        MarketSide recommendedSide = upEv >= downEv
                ? MarketSide.UP
                : MarketSide.DOWN;

        double recommendedChance = recommendedSide == MarketSide.UP
                ? upChance
                : downChance;

        double recommendedEv = Math.max(upEv, downEv);

        return new UpDownEvEstimate(
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
     * Estimates the probability that the final 60-second TWAP will be
     * above the opening/reference price.
     *
     * The model works in two stages:
     *
     * 1. Use the probability table to estimate where the LIVE BTC price
     *    can be when the market resolves.
     *
     * 2. Convert that future live price into the expected final 60s TWAP,
     *    taking into account how many seconds are left.
     *
     * The closer we get to the end of the market, the less influence the
     * future live price has on the final TWAP because most of the 60-second
     * averaging window has already happened.
     */
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

        secondsLeft = Math.max(0, secondsLeft);

        /*
         * If there is more than one complete TWAP window remaining,
         * the current TWAP has effectively lost its importance.
         *
         * The future live price is the important variable.
         */
        if (secondsLeft >= TWAP_WINDOW_SECONDS) {
            double requiredPctChange = percentageChange(
                    currentLivePrice,
                    referencePrice
            );

            return probabilityOfReaching(
                    requiredPctChange,
                    secondsLeft
            );
        }

        /*
         * We have less than 60 seconds remaining.
         *
         * The current TWAP is already partially determined by historical
         * prices. Only the observations arriving during the remaining
         * seconds can move it toward the future live price.
         *
         * Approximation:
         *
         *     finalTWAP =
         *         currentTWAP * (1 - futureWeight)
         *         +
         *         futureLivePrice * futureWeight
         *
         * where:
         *
         *     futureWeight = secondsLeft / 60
         *
         * Therefore we calculate which future LIVE price would be required
         * for the final TWAP to reach the strike.
         */
        double futureWeight =
                (double) secondsLeft / TWAP_WINDOW_SECONDS;

        /*
         * At exactly zero seconds there is no future data that can change
         * the TWAP anymore.
         */
        if (futureWeight <= 0.0) {
            return currentTwapPrice.compareTo(referencePrice) > 0
                    ? 1.0
                    : 0.0;
        }

        /*
         * Solve:
         *
         * reference =
         *     currentTwap * (1 - w)
         *     + futureLive * w
         *
         * for futureLive:
         *
         * futureLive =
         *     (reference - currentTwap * (1 - w)) / w
         */
        double currentTwap = currentTwapPrice.doubleValue();
        double reference = referencePrice.doubleValue();

        double requiredFutureLivePrice =
                (reference - currentTwap * (1.0 - futureWeight))
                        / futureWeight;

        /*
         * We now ask the probability table:
         *
         * "What is the probability that live BTC reaches this price
         *  within the remaining N seconds?"
         */
        double requiredPctChange =
                ((requiredFutureLivePrice - currentLivePrice.doubleValue())
                        / currentLivePrice.doubleValue())
                        * 100.0;

        return probabilityOfReaching(
                requiredPctChange,
                secondsLeft
        );
    }

    /**
     * Converts a required percentage movement of the live BTC price into
     * the probability supplied by the historical probability table.
     */
    private double probabilityOfReaching(
            double requiredPctChange,
            int secondsLeft
    ) {
        secondsLeft = Math.max(0, Math.min(secondsLeft, 300));

        /*
         * If the required movement is zero or negative, the current/live
         * price is already on the desired side of the target.
         */
        if (requiredPctChange <= 0.0) {
            double probabilityDown =
                    table.getChance(secondsLeft, Math.abs(requiredPctChange));

            return 1.0 - probabilityDown;
        }

        return table.getChance(secondsLeft, requiredPctChange);
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

    private double evForSide(
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
}