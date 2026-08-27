package al.r1.polytrader.engine;

import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
public class ProbabilityTable {

    /**
     * Table representing how often a given price change occurs within a given
     * amount of time.
     *
     * Structure:
     *
     * Price change bucket
     *            -3%   | -2.9% | ...   | 2.9% | 3%+
     * --------------------------------
     * 1s       |  ...  |  ...  |  ...  |
     * 2s       |  ...  |  ...  |  ...  |
     * 3s       |  ...  |  ...  |  ...  |
     * ...      |  ...  |  ...  |  ...  |
     * 300s     |  ...  |  ...  |  ...  |
     *
     * probabilitiesTable[111][30]
     * - weighted number of occurrences where price changed by 0%
     * within 111 seconds
     *
     *
     * probabilitiesTable[22][0]
     * - weighted number of occurrences where price changed by -3% or more
     * within 22 seconds
     *
     * probabilitiesTable[288][60]
     * - weighted number of occurrences where price changed by +3% or more
     * within 288 seconds
     *
     * probabilitiesTable[300][35]
     * - weighted number of occurrences where price changed by +0.5%
     * within 300 seconds
     *
     * The values are doubles because observations are weighted.
     * Newer observations can receive a higher weight than older observations,
     * allowing the table to adapt to recent market behavior while still
     * retaining historical data.
     */
    @Getter
    private double[][] probabilitiesTable;
    @Getter
    private int numberOfChecks;
    @Getter
    private double numberOfChecksWithWeight;

    public ProbabilityTable() {
        this.probabilitiesTable = new double[301][61];
        this.numberOfChecks = 0;
        this.numberOfChecksWithWeight = 0;
    }

    public void updateProbabilitiesTable(int time, double changePure) {
        int changeArea = mapChangeArea(changePure);
        double weight = Math.max((double) numberOfChecks / 1000000, 1);
        if (changeArea == 30) {
            probabilitiesTable[time][30] += weight;
        } else if (changeArea < 30) {
            for (int i = 0; i < changeArea; i++) {
                probabilitiesTable[time][i] += weight;
            }
        } else {
            for (int i = 31; i < changeArea; i++) {
                probabilitiesTable[time][i] += weight;
            }
        }
        numberOfChecksWithWeight += weight;
        numberOfChecks++;
    }

    public int mapChangeArea(double changePure) {
        int change = (int) (changePure * 1000) - 1000;
        if (change > 0) {
            int position = 30 + (change);
            return Math.min(60, position);
        } else if (change < 0) {
            int position = 30 - change;
            return Math.max(1, position);
        } else {
            return 0;
        }
    }
}
