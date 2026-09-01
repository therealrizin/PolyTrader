package al.r1.polytrader.engine;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    private static final int SECONDS_DIM = 301;

    /**
     * Price-change resolution:
     *
     * -0.500% ... 0.000% ... +0.500%
     * in 0.001% increments.
     */
    private static final int BUCKET_RANGE = 500;
    private static final int CENTER = BUCKET_RANGE;
    private static final int BUCKET_COUNT = 2 * BUCKET_RANGE + 1;

    @Getter
    private double[][] probabilitiesTable;

    /**
     * Number of independent observations added to the table.
     */
    @Getter
    private int numberOfChecks;

    /**
     * Sum of weights of all independent observations.
     *
     * This is the correct denominator for probabilities.
     *
     * IMPORTANT:
     * Do NOT calculate the denominator by summing a row of
     * probabilitiesTable because each observation is inserted into
     * multiple cumulative buckets.
     */
    @Getter
    private double numberOfChecksWithWeight;

    private double weight;

    public ProbabilityTable() {
        reset();
    }

    /**
     * Returns the probability associated with the requested price-change
     * threshold.
     *
     * The table stores cumulative directional probabilities:
     *
     * Positive threshold:
     *
     *     getChance(300, +0.100)
     *
     * means approximately:
     *
     *     P(price change >= +0.100% within 300 seconds)
     *
     * Negative threshold:
     *
     *     getChance(300, -0.100)
     *
     * means approximately:
     *
     *     P(price change <= -0.100% within 300 seconds)
     *
     * NOTE:
     * For exactly 0.000%, the CENTER bucket only contains exact-zero
     * observations. Therefore callers should not interpret
     * getChance(..., 0.0) as P(change <= 0) or P(change >= 0).
     */
    public synchronized double getChance(double seconds, double changePercent) {
        int time = (int) Math.round(seconds);

        if (time < 1 || time > 300) {
            return 0.0;
        }

        if (Double.isNaN(changePercent) || Double.isInfinite(changePercent)) {
            return 0.0;
        }

        int bucket = mapPercentToBucket(changePercent);

        double weightedCount = probabilitiesTable[time][bucket];

        if (numberOfChecksWithWeight <= 0.0) {
            return 0.0;
        }

        double probability = weightedCount / numberOfChecksWithWeight;

        // Numerical protection.
        return Math.max(0.0, Math.min(1.0, probability));
    }

    /**
     * Adds one observation to the cumulative probability table.
     *
     * changePure is a price ratio:
     *
     *     1.00100 = +0.100%
     *     0.99900 = -0.100%
     *
     * For a positive move, all positive thresholds up to the observed move
     * are incremented.
     *
     * Example:
     *
     *     observed move = +0.100%
     *
     * increments:
     *
     *     +0.001%
     *     +0.002%
     *     ...
     *     +0.100%
     *
     * For a negative move, all negative thresholds down to the observed
     * move are incremented.
     */
    public synchronized void updateProbabilitiesTable(
            int time,
            double changePure,
            boolean newRecord
    ) {
        if (time < 1 || time > 300) {
            return;
        }

        if (Double.isNaN(changePure)
                || Double.isInfinite(changePure)
                || changePure <= 0.0) {
            return;
        }

        int changeArea = mapChangeArea(changePure);

        if (changeArea > CENTER) {

            // Positive move.
            //
            // Example +0.100%:
            // increment +0.001% ... +0.100%
            for (int i = CENTER + 1; i <= changeArea; i++) {
                probabilitiesTable[time][i] += weight;
            }

        } else if (changeArea < CENTER) {

            // Negative move.
            //
            // Example -0.100%:
            // increment -0.001% ... -0.100%
            for (int i = CENTER - 1; i >= changeArea; i--) {
                probabilitiesTable[time][i] += weight;
            }

        } else {

            // Exact zero movement.
            //
            // This is deliberately NOT added to the neighbouring
            // cumulative buckets.
            probabilitiesTable[time][CENTER] += weight;
        }

        if (newRecord) {
            updateNumberOfChecks();
        }
    }

    /**
     * Registers one independent observation and updates the weight used
     * for future observations.
     *
     * The weighting scheme is preserved from the original implementation:
     *
     *     weight = max(numberOfChecks / 1,000,000, 1)
     *
     * Therefore older observations remain represented while newer
     * observations gradually receive more weight.
     */
    public synchronized void updateNumberOfChecks() {
        this.weight = Math.max(
                (double) numberOfChecks / 1_000_000.0,
                1.0
        );

        this.numberOfChecksWithWeight += weight;
        this.numberOfChecks++;
    }

    /**
     * Converts a price ratio to a bucket.
     *
     * Examples:
     *
     *     1.00000 -> 500
     *     1.00001 -> 501 (+0.001%)
     *     1.00100 -> 600 (+0.100%)
     *     0.99999 -> 499 (-0.001%)
     *     0.99900 -> 400 (-0.100%)
     */
    public int mapChangeArea(double changePure) {
        if (Double.isNaN(changePure) || Double.isInfinite(changePure)) {
            return CENTER;
        }

        int changeUnits = (int) Math.round(
                (changePure - 1.0) * 100_000.0
        );

        if (changeUnits <= -BUCKET_RANGE) {
            return 0;
        }

        if (changeUnits >= BUCKET_RANGE) {
            return BUCKET_COUNT - 1;
        }

        return CENTER + changeUnits;
    }

    /**
     * Converts a percentage value to the corresponding bucket.
     *
     * Example:
     *
     *     +0.100 -> bucket 600
     *     +0.001 -> bucket 501
     *      0.000 -> bucket 500
     *     -0.001 -> bucket 499
     *     -0.100 -> bucket 400
     */
    private int mapPercentToBucket(double changePercent) {
        int bucket = (int) Math.round(changePercent * 1000.0);

        bucket += CENTER;

        return Math.max(
                0,
                Math.min(BUCKET_COUNT - 1, bucket)
        );
    }

    public synchronized void reset() {
        this.probabilitiesTable =
                new double[SECONDS_DIM][BUCKET_COUNT];

        this.numberOfChecks = 0;
        this.numberOfChecksWithWeight = 0.0;
        this.weight = 1.0;
    }
}