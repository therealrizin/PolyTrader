package al.r1.polytrader.engine;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    private static final int SECONDS_DIM = 301;
    private static final int BUCKET_RANGE = 500;   // ±0.50% in 0.001% steps
    private static final int CENTER = BUCKET_RANGE;
    private static final int BUCKET_COUNT = 2 * BUCKET_RANGE + 1; // 1001 buckets

    /**
     * Table representing how often a given price change occurs within a given
     * amount of time, at 0.001% resolution.
     *
     * Structure:
     *
     * Price change bucket
     *            -0.500% | -0.499% | ... | 0.000% | ... | +0.499% | +0.500%
     * --------------------------------
     * 1s       |   ...   |  ...   | ... |  ...  | ... |  ...   |  ...
     * ...
     * 300s     |   ...   |  ...   | ... |  ...  | ... |  ...   |  ...
     *
     * Bucket semantics are cumulative toward zero:
     * - A move of +0.333% increments buckets +0.001% through +0.333% (inclusive).
     * - A move of -0.499% increments buckets -0.001% through -0.499% (inclusive).
     * - Index 0 ("-0.500% or more") and index 1000 ("+0.500% or more") are
     *   catch-alls for anything beyond the tracked range.
     * - Index 500 (0.000%) only increments on an exact-zero change.
     *
     * probabilitiesTable[time][bucket] = weighted count of occurrences.
     *
     * The values are doubles because observations are weighted — newer
     * observations can receive a higher weight than older ones.
     */
    @Getter
    private double[][] probabilitiesTable;
    @Getter
    private int numberOfChecks;
    @Getter
    private double numberOfChecksWithWeight;
    private double weight;

    public ProbabilityTable() {
        this.probabilitiesTable = new double[SECONDS_DIM][BUCKET_COUNT];
        this.numberOfChecks = 0;
        this.numberOfChecksWithWeight = 0;
        this.weight = 1;
    }

    public double getChance(double seconds, double chance) {
        int time = (int) Math.round(seconds);

        if (time < 1 || time > 300) {
            return 0.0;
        }

        // 0.001% resolution, with 0.000% at index 500
        int bucket = (int) Math.round(chance * 1000) + 500;

        // Clamp to catch-all buckets
        bucket = Math.max(0, Math.min(1000, bucket));

        double value = probabilitiesTable[time][bucket];

        // Total observations for this time
        double total = 0.0;
        for (double count : probabilitiesTable[time]) {
            total += count;
        }

        if (total == 0.0) {
            return 0.0;
        }

        return value / total;
    }

    public void updateProbabilitiesTable(int time, double changePure, boolean newRecord) {
        int changeArea = mapChangeArea(changePure);

        if (changeArea > CENTER) {
            for (int i = CENTER + 1; i <= changeArea; i++) {
                probabilitiesTable[time][i] += weight;
            }
        } else if (changeArea < CENTER) {
            for (int i = CENTER - 1; i >= changeArea; i--) {
                probabilitiesTable[time][i] += weight;
            }
        } else {
            probabilitiesTable[time][CENTER] += weight;
        }

        if (newRecord) {
            updateNumberOfChecks();
        }
    }

    public void updateNumberOfChecks() {
        this.weight = Math.max((double) numberOfChecks / 1_000_000, 1);
        this.numberOfChecksWithWeight += weight;
        this.numberOfChecks++;
    }

    /**
     * Maps a price ratio (e.g. 1.00333 for +0.333%) to a bucket index in
     * [0, 1000], where each step is 0.001% and index 500 is 0.000%.
     */
    public int mapChangeArea(double changePure) {
        int changeUnits = (int) Math.round((changePure - 1) * 100000);
        if (changeUnits <= -BUCKET_RANGE) return 0;
        if (changeUnits >= BUCKET_RANGE) return BUCKET_COUNT - 1;
        return CENTER + changeUnits;
    }

    public void reset() {
        this.probabilitiesTable = new double[SECONDS_DIM][BUCKET_COUNT];
        this.numberOfChecks = 0;
        this.numberOfChecksWithWeight = 0;
        this.weight = 1;
    }
}