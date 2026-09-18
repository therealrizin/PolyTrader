package al.r1.polytrader.engine;

import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    private static final int SECONDS_DIM = 301;
    private static final int BUCKET_RANGE = 500;
    private static final int CENTER = BUCKET_RANGE;
    private static final int BUCKET_COUNT = 2 * BUCKET_RANGE + 1;
    public static final int TREND_LAYER_COUNT = 27;
    public static final int NEUTRAL_TREND = 13;

    private double[][][] probabilitiesTable;

    private int[] numberOfChecks;

    private double[] numberOfChecksWithWeight;

    private double[] weights;

    public ProbabilityTable() {
        reset();
    }

    public synchronized double getChance(double seconds, double changePercent) {
        return getChance(seconds, changePercent, NEUTRAL_TREND);
    }

    public synchronized double getChance(double seconds, double changePercent, int trendLayer) {
        int time = (int) Math.round(seconds);

        if (time < 1 || time > 300 || !isValidTrendLayer(trendLayer)
                || Double.isNaN(changePercent) || Double.isInfinite(changePercent)) {
            return 0.0;
        }

        if (changePercent < 0.001 && changePercent > -0.001) {
            return 0.5;
        }

        int bucket = mapPercentToBucket(changePercent);
        double weightedCount = probabilitiesTable[trendLayer][time][bucket];
        if (numberOfChecksWithWeight[trendLayer] <= 0.0) {
            return 0.0;
        }
        double probability = weightedCount / numberOfChecksWithWeight[trendLayer];
        return Math.clamp(probability, 0.0, 1.0);
    }

    public synchronized void updateProbabilitiesTable(
            int time,
            double changePure,
            boolean newRecord
    ) {
        updateProbabilitiesTable(time, changePure, newRecord, NEUTRAL_TREND);
    }

    public synchronized void updateProbabilitiesTable(
            int time,
            double changePure,
            boolean newRecord,
            int trendLayer
    ) {
        if (time < 1 || time > 300) {
            return;
        }

        if (!isValidTrendLayer(trendLayer) || Double.isNaN(changePure) || Double.isInfinite(changePure) || changePure <= 0.0) {
            return;
        }

        int changeArea = mapChangeArea(changePure);

        if (changeArea > CENTER) {
            for (int i = CENTER + 1; i <= changeArea; i++) {
                probabilitiesTable[trendLayer][time][i] += weights[trendLayer];
            }
        } else if (changeArea < CENTER) {
            for (int i = CENTER - 1; i >= changeArea; i--) {
                probabilitiesTable[trendLayer][time][i] += weights[trendLayer];
            }
        } else {
            probabilitiesTable[trendLayer][time][CENTER] += weights[trendLayer];
        }

        if (newRecord) {
            updateNumberOfChecks(trendLayer);
        }
    }

    public synchronized void updateNumberOfChecks() {
        updateNumberOfChecks(NEUTRAL_TREND);
    }

    public synchronized void updateNumberOfChecks(int trendLayer) {
        if (!isValidTrendLayer(trendLayer)) {
            return;
        }
        weights[trendLayer] = Math.max(
                (double) numberOfChecks[trendLayer] / 1_000_000.0,
                1.0
        );

        this.numberOfChecksWithWeight[trendLayer] += weights[trendLayer];
        this.numberOfChecks[trendLayer]++;
    }

    public int getNumberOfChecks() {
        int total = 0;
        for (int checks : numberOfChecks) total += checks;
        return total;
    }

    public int getNumberOfChecks(int trendLayer) {
        return isValidTrendLayer(trendLayer) ? numberOfChecks[trendLayer] : 0;
    }

    public double getNumberOfChecksWithWeight() {
        double total = 0.0;
        for (double checksWithWeight : numberOfChecksWithWeight) total += checksWithWeight;
        return total;
    }

    public double getNumberOfChecksWithWeight(int trendLayer) {
        return isValidTrendLayer(trendLayer) ? numberOfChecksWithWeight[trendLayer] : 0.0;
    }

    public double[][] getProbabilitiesTable() {
        return getProbabilitiesTable(NEUTRAL_TREND);
    }

    public double[][] getProbabilitiesTable(int trendLayer) {
        return isValidTrendLayer(trendLayer) ? probabilitiesTable[trendLayer] : new double[SECONDS_DIM][BUCKET_COUNT];
    }

    public static int trendLayer(int oldestTick, int middleTick, int newestTick) {
        if (!isTick(oldestTick) || !isTick(middleTick) || !isTick(newestTick)) {
            return NEUTRAL_TREND;
        }
        return (oldestTick + 1) * 9 + (middleTick + 1) * 3 + newestTick + 1;
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
        return Math.clamp(bucket, 0, BUCKET_COUNT - 1);
    }

    public synchronized void reset() {
        this.probabilitiesTable =
                new double[TREND_LAYER_COUNT][SECONDS_DIM][BUCKET_COUNT];
        this.numberOfChecks = new int[TREND_LAYER_COUNT];
        this.numberOfChecksWithWeight = new double[TREND_LAYER_COUNT];
        this.weights = new double[TREND_LAYER_COUNT];
        java.util.Arrays.fill(this.weights, 1.0);
    }

    private static boolean isTick(int tick) {
        return tick >= -1 && tick <= 1;
    }

    private static boolean isValidTrendLayer(int trendLayer) {
        return trendLayer >= 0 && trendLayer < TREND_LAYER_COUNT;
    }
}
