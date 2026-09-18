package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuntimeTradingSettingsTest {

    @Test
    void appliesPartialUpdatesWithoutChangingOtherSettings() {
        RuntimeTradingSettings settings = new RuntimeTradingSettings(properties());

        var updated = settings.update(new RuntimeTradingSettings.Update(0.70, 0.55, null, null, null, 0.05, null));

        assertEquals(0.70, updated.minimumExpectedEv());
        assertEquals(0.55, updated.minimumWinChance());
        assertEquals(0.05, updated.minimumExecutableEdge());
        assertEquals(25, updated.minimumTrendSamples());
        assertEquals(0.10, updated.maximumBookSpread());
    }

    @Test
    void rejectsUnsafeSettingsAtomically() {
        RuntimeTradingSettings settings = new RuntimeTradingSettings(properties());

        assertThrows(IllegalArgumentException.class,
                () -> settings.update(new RuntimeTradingSettings.Update(null, 1.0, null, null, null, null, null)));
        assertEquals(0.40, settings.get().minimumWinChance());
    }

    private TradingProperties properties() {
        return new TradingProperties(true, 0.5, 0.4, 0.07, BigDecimal.ONE, 10, 1.3,
                25, 30, 1500, 0.02, 0.10, "test-token");
    }
}
