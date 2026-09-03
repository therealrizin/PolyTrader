package al.r1.polytrader.engine;

import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private static final int TWAP_WINDOW_SECONDS = 60;

    /*
     * Movement smaller than half of the 0.001% bucket is considered
     * directionless.
     */
    private static final double ZERO_MOVEMENT_THRESHOLD_PERCENT = 0.0005;

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

        MarketSide recommendedSide =
                upEv >= downEv
                        ? MarketSide.UP
                        : MarketSide.DOWN;

        double recommendedChance =
                recommendedSide == MarketSide.UP
                        ? upChance
                        : downChance;

        double recommendedEv =
                Math.max(upEv, downEv);

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
     * Returns the probability that the final Chainlink 60-second TWAP
     * will finish above the opening/reference price.
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

        secondsLeft = Math.clamp(
                secondsLeft,
                0,
                300
        );

        /*
         * If >= 60 seconds remain, the current TWAP has effectively
         * disappeared from the final TWAP.
         */
        if (secondsLeft >= TWAP_WINDOW_SECONDS) {

            double requiredPctChange =
                    percentageChange(
                            currentLivePrice,
                            referencePrice
                    );

            return probabilityOfReaching(
                    requiredPctChange,
                    secondsLeft
            );
        }

        /*
         * Less than one minute remains.
         *
         * Approximate the final TWAP as:
         *
         * finalTWAP =
         *     currentTWAP * (1 - w)
         *     + futureLivePrice * w
         *
         * where w = secondsLeft / 60.
         */
        double futureWeight =
                (double) secondsLeft / TWAP_WINDOW_SECONDS;

        if (futureWeight <= 0.0) {

            return currentTwapPrice.compareTo(referencePrice) > 0
                    ? 1.0
                    : 0.0;
        }

        double currentTwap =
                currentTwapPrice.doubleValue();

        double reference =
                referencePrice.doubleValue();

        /*
         * Solve:
         *
         * reference =
         *     currentTwap * (1 - w)
         *     + futureLive * w
         */
        double requiredFutureLivePrice =
                (
                        reference
                                - currentTwap * (1.0 - futureWeight)
                ) / futureWeight;

        double currentLive =
                currentLivePrice.doubleValue();

        double requiredPctChange =
                (
                        (requiredFutureLivePrice - currentLive)
                                / currentLive
                ) * 100.0;

        return probabilityOfReaching(
                requiredPctChange,
                secondsLeft
        );
    }

    private double probabilityOfReaching(
            double requiredPctChange,
            int secondsLeft
    ) {
        secondsLeft = Math.clamp(
                secondsLeft,
                0,
                300
        );

        /*
         * No meaningful directional movement required.
         */
        if (Math.abs(requiredPctChange)
                < ZERO_MOVEMENT_THRESHOLD_PERCENT) {

            return 0.5;
        }

        /*
         * We need DOWN movement.
         *
         * getChance() returns the probability of reaching the requested
         * magnitude in that direction.
         *
         * Therefore:
         *
         * P(UP) = 1 - P(DOWN)
         */
        if (requiredPctChange < 0.0) {

            double probabilityDown =
                    table.getChance(
                            secondsLeft,
                            Math.abs(requiredPctChange)
                    );

            return clampProbability(
                    1.0 - probabilityDown
            );
        }

        /*
         * We need UP movement.
         */
        double probabilityUp =
                table.getChance(
                        secondsLeft,
                        requiredPctChange
                );

        return clampProbability(
                probabilityUp
        );
    }

    private double percentageChange(
            BigDecimal from,
            BigDecimal to
    ) {
        return to.subtract(from)
                .divide(
                        from,
                        8,
                        RoundingMode.HALF_UP
                )
                .multiply(
                        BigDecimal.valueOf(100)
                )
                .doubleValue();
    }

    /**
     * EV of BUYING a share at marketPrice.
     *
     * This is the same entry EV used by the trading decision.
     */
    private double evForBuySide(
            double winChance,
            double marketPrice,
            double takerFee
    ) {
        if (marketPrice <= 0.0
                || marketPrice >= 1.0) {

            return Double.NEGATIVE_INFINITY;
        }

        /*
         * Binary share pays $1 if it wins.
         *
         * Gross expected payout per $1 invested:
         *
         *     winChance / price
         *
         * The fee is applied to the trade at match time.
         *
         * This retains the same general EV convention your bot
         * currently uses while keeping the fee parameter explicit.
         */
        double grossPayout =
                1.0 / marketPrice;

        double netPayout =
                1.0
                        + (grossPayout - 1.0)
                        * (1.0 - takerFee);

        return winChance * netPayout - 1.0;
    }

    /**
     * Returns the value of HOLDING one share to resolution.
     *
     * A winning share pays $1.
     * A losing share pays $0.
     *
     * Therefore:
     *
     *     EV(hold) = P(win)
     */
    public double holdValuePerShare(
            double winChance
    ) {
        return clampProbability(winChance);
    }

    /**
     * Calculates how much better/worse SELLING one share now is compared
     * with HOLDING that same share to resolution.
     *
     * Positive:
     *
     *     selling now > expected value of holding
     *
     * Negative:
     *
     *     holding > selling now
     *
     * This is deliberately expressed per share rather than relative to
     * the original purchase price.
     */
    public double sellAdvantagePerShare(
            double winChance,
            double currentBid,
            double takerFee
    ) {
        if (currentBid <= 0.0
                || currentBid >= 1.0) {

            return Double.NEGATIVE_INFINITY;
        }

        double holdValue =
                holdValuePerShare(winChance);

        double sellValue =
                netSellValuePerShare(
                        currentBid,
                        takerFee
                );

        return sellValue - holdValue;
    }

    /**
     * Expected net value received for one share when selling at the
     * current bid.
     *
     * Polymarket crypto taker fees are calculated as:
     *
     *     fee = shares * rate * p * (1-p)
     *
     * Therefore for one share:
     *
     *     fee = rate * p * (1-p)
     */
    public double netSellValuePerShare(
            double currentBid,
            double takerFee
    ) {
        if (currentBid <= 0.0
                || currentBid >= 1.0) {

            return 0.0;
        }

        double fee =
                takerFee
                        * currentBid
                        * (1.0 - currentBid);

        return currentBid - fee;
    }

    private double clampProbability(
            double probability
    ) {
        if (Double.isNaN(probability)
                || Double.isInfinite(probability)) {

            return 0.5;
        }

        return Math.clamp(
                probability,
                0.0,
                1.0
        );
    }
}