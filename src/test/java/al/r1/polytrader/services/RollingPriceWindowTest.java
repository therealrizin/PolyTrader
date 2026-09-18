package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollingPriceWindowTest {

    @Test
    void countsOneObservationPerIncomingPriceNotPerHistoricalComparison() {
        RollingPriceWindow window = new RollingPriceWindow();
        ProbabilityTable table = new ProbabilityTable();

        window.addAndUpdateTable(1_000, 100.0, table);
        window.addAndUpdateTable(2_000, 101.0, table);
        window.addAndUpdateTable(3_000, 102.0, table);

        assertEquals(3, table.getNumberOfChecks());
    }

    @Test
    void keepsProbabilityObservationsInTheirThreeTickTrendLayer() {
        ProbabilityTable table = new ProbabilityTable();
        int risingTrend = ProbabilityTable.trendLayer(1, 1, 1);
        int fallingTrend = ProbabilityTable.trendLayer(-1, -1, -1);

        table.updateNumberOfChecks(risingTrend);
        table.updateProbabilitiesTable(1, 1.001, false, risingTrend);

        assertEquals(1, table.getNumberOfChecks(risingTrend));
        assertEquals(0, table.getNumberOfChecks(fallingTrend));
        assertEquals(1.0, table.getChance(1, 0.001, risingTrend));
        assertEquals(0.0, table.getChance(1, 0.001, fallingTrend));
    }

    @Test
    void updatesTheMatchingLayerFromCompletedBinanceSeconds() {
        BinanceRollingWindow window = new BinanceRollingWindow();
        ProbabilityTable table = new ProbabilityTable();

        window.record(1_000, BigDecimal.valueOf(100), table);
        window.record(2_000, BigDecimal.valueOf(101), table);
        window.record(3_000, BigDecimal.valueOf(102), table);
        window.record(4_000, BigDecimal.valueOf(101), table);
        var completed = window.record(5_000, BigDecimal.valueOf(100), table);

        int trendLayer = ProbabilityTable.trendLayer(1, 1, -1);
        assertTrue(completed.isPresent());
        assertEquals(BigDecimal.valueOf(101), completed.get());
        assertEquals(1, table.getNumberOfChecks(trendLayer));
    }
}
