package al.r1.polytrader.services;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrossVenuePriceCalibratorTest {

    @Test
    void learnsAPartialChainlinkResponseToBinanceReturns() {
        CrossVenuePriceCalibrator calibrator = new CrossVenuePriceCalibrator();
        BigDecimal binance = BigDecimal.valueOf(100_000);
        BigDecimal chainlink = BigDecimal.valueOf(100_000);

        calibrator.observe(binance, chainlink);
        for (int i = 0; i < 60; i++) {
            binance = binance.multiply(BigDecimal.valueOf(1.001));
            // Chainlink has a 50% response to each Binance return.
            chainlink = chainlink.multiply(BigDecimal.valueOf(1.0005));
            calibrator.observe(binance, chainlink);
        }

        CrossVenuePriceCalibrator.Calibration calibration = calibrator.observe(binance, chainlink);
        assertTrue(calibration.samples() >= 60);
        assertTrue(calibration.beta() > 0.40 && calibration.beta() < 0.60);

        BigDecimal projected = calibrator.project(BigDecimal.valueOf(100), BigDecimal.valueOf(1.01));
        assertEquals(100.5, projected.doubleValue(), 0.15);
    }

    @Test
    void boundsAnUnstableEstimate() {
        CrossVenuePriceCalibrator calibrator = new CrossVenuePriceCalibrator();
        calibrator.observe(BigDecimal.valueOf(100), BigDecimal.valueOf(100));
        CrossVenuePriceCalibrator.Calibration calibration = calibrator.observe(BigDecimal.valueOf(101), BigDecimal.valueOf(110));

        assertTrue(calibration.beta() >= 0.0 && calibration.beta() <= 1.5);
    }
}
