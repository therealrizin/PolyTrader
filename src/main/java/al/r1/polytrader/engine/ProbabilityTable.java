package al.r1.polytrader.engine;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    private static final int SECONDS_DIM = 301;
    private static final int BUCKET_RANGE = 50; // ±0.50% in 0.01% steps
    private static final int CENTER = BUCKET_RANGE; // index of 0.00%
    private static final int BUCKET_COUNT = BUCKET_RANGE * 2 + 1; // 101 buckets

    /**
     * Table representing how often a given price change occurs within a given
     * amount of time, at 0.01% resolution.
     *
     * Structure:
     *
     * Price change bucket
     *            -0.50%  | -0.49% | ... | 0.00% | ... | +0.49% | +0.50%
     * --------------------------------
     * 1s       |   ...   |  ...   | ... |  ...  | ... |  ...   |  ...
     * ...
     * 300s     |   ...   |  ...   | ... |  ...  | ... |  ...   |  ...
     *
     * Bucket semantics are cumulative toward zero:
     * - A move of +0.33% increments buckets +0.01% through +0.33% (inclusive).
     * - A move of -0.49% increments buckets -0.01% through -0.49% (inclusive).
     * - Index 0 ("-0.50% or more") and index 100 ("+0.50% or more") are
     *   catch-alls for anything beyond the tracked range.
     * - Index 50 (0.00%) only increments on an exact-zero change.
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
     * Maps a price ratio (e.g. 1.0033 for +0.33%) to a bucket index in
     * [0, 100], where each step is 0.01% and index 50 is 0.00%.
     */
    public int mapChangeArea(double changePure) {
        int changeUnits = (int) Math.round((changePure - 1) * 10000);
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