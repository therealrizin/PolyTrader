package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
