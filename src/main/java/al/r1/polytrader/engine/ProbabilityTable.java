package al.r1.polytrader.engine;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    private static final int SECONDS_DIM = 301;
    private static final int BUCKET_RANGE = 500;
    private static final int CENTER = BUCKET_RANGE;
    private static final int BUCKET_COUNT = 2 * BUCKET_RANGE + 1;

    @Getter
    private double[][] probabilitiesTable;

    @Getter
    private int numberOfChecks;

    @Getter
    private double numberOfChecksWithWeight;

    private double weight;

    public ProbabilityTable() {
        reset();
    }

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

        return Math.clamp(probability, 0.0, 1.0);
    }

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

    public synchronized void updateNumberOfChecks() {
        this.weight = Math.max(
                (double) numberOfChecks / 1_000_000.0,
                1.0
        );

        this.numberOfChecksWithWeight += weight;
        this.numberOfChecks++;
    }

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

    private int mapPercentToBucket(double changePercent) {
        int bucket = (int) Math.round(changePercent * 1000.0);

        bucket += CENTER;

        return Math.clamp(bucket,
                0, BUCKET_COUNT - 1);
    }

    public synchronized void reset() {
        this.probabilitiesTable =
                new double[SECONDS_DIM][BUCKET_COUNT];

        this.numberOfChecks = 0;
        this.numberOfChecksWithWeight = 0.0;
        this.weight = 1.0;
    }
}