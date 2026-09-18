package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/** Mutable, runtime-only risk settings. Restarting restores values from configuration. */
@Component
public class RuntimeTradingSettings {
    private final AtomicReference<Settings> current;

    public RuntimeTradingSettings(TradingProperties properties) {
        current = new AtomicReference<>(new Settings(properties.minimumExpectedEv(), properties.minimumWinChance(),
                properties.minimumTrendSamples(), properties.minimumCalibrationSamples(),
                properties.maximumCrossVenueSkewMillis(), properties.minimumExecutableEdge(), properties.maximumBookSpread()));
    }

    public Settings get() { return current.get(); }

    public synchronized Settings update(Update request) {
        if (request == null) throw new IllegalArgumentException("request body is required");
        Settings old = current.get();
        Settings next = new Settings(d(request.minimumExpectedEv(), old.minimumExpectedEv()),
                d(request.minimumWinChance(), old.minimumWinChance()),
                i(request.minimumTrendSamples(), old.minimumTrendSamples()),
                i(request.minimumCalibrationSamples(), old.minimumCalibrationSamples()),
                l(request.maximumCrossVenueSkewMillis(), old.maximumCrossVenueSkewMillis()),
                d(request.minimumExecutableEdge(), old.minimumExecutableEdge()),
                d(request.maximumBookSpread(), old.maximumBookSpread()));
        validate(next);
        current.set(next);
        return next;
    }

    private void validate(Settings s) {
        range(s.minimumExpectedEv(), 0, 5, "minimumExpectedEv");
        range(s.minimumWinChance(), 0.01, 0.99, "minimumWinChance");
        range(s.minimumExecutableEdge(), 0, 0.99, "minimumExecutableEdge");
        range(s.maximumBookSpread(), 0.001, 1, "maximumBookSpread");
        if (s.minimumTrendSamples() < 1 || s.minimumCalibrationSamples() < 1) throw new IllegalArgumentException("sample counts must be positive");
        if (s.maximumCrossVenueSkewMillis() < 50 || s.maximumCrossVenueSkewMillis() > 60_000) throw new IllegalArgumentException("maximumCrossVenueSkewMillis must be between 50 and 60000");
    }

    private void range(double value, double min, double max, String name) {
        if (!Double.isFinite(value) || value < min || value > max) throw new IllegalArgumentException(name + " must be between " + min + " and " + max);
    }
    private double d(Double value, double fallback) { return value == null ? fallback : value; }
    private int i(Integer value, int fallback) { return value == null ? fallback : value; }
    private long l(Long value, long fallback) { return value == null ? fallback : value; }

    public record Settings(double minimumExpectedEv, double minimumWinChance, int minimumTrendSamples,
                           int minimumCalibrationSamples, long maximumCrossVenueSkewMillis,
                           double minimumExecutableEdge, double maximumBookSpread) {}
    public record Update(Double minimumExpectedEv, Double minimumWinChance, Integer minimumTrendSamples,
                         Integer minimumCalibrationSamples, Long maximumCrossVenueSkewMillis,
                         Double minimumExecutableEdge, Double maximumBookSpread) {}
}
