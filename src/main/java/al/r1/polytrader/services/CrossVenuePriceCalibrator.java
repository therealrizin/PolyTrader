package al.r1.polytrader.services;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Learns how much a one-second Binance return is reflected in the subsequent
 * Chainlink return.  It deliberately models returns, not absolute prices: BTC/USDT
 * and BTC/USD can have a persistent basis without making the signal unusable.
 */
@Component
public class CrossVenuePriceCalibrator {

    private static final double DECAY = 0.05;
    private static final double MIN_VARIANCE = 1.0e-12;
    private static final double MIN_BETA = 0.0;
    private static final double MAX_BETA = 1.5;

    private Double previousBinance;
    private Double previousChainlink;
    private double meanBinanceReturn;
    private double meanChainlinkReturn;
    private double covariance;
    private double variance;
    private int samples;

    public synchronized Calibration observe(BigDecimal binancePrice, BigDecimal chainlinkPrice) {
        if (!valid(binancePrice) || !valid(chainlinkPrice)) return snapshot();

        double binance = binancePrice.doubleValue();
        double chainlink = chainlinkPrice.doubleValue();
        if (previousBinance != null && previousChainlink != null) {
            double x = Math.log(binance / previousBinance);
            double y = Math.log(chainlink / previousChainlink);
            if (Double.isFinite(x) && Double.isFinite(y)) {
                double dx = x - meanBinanceReturn;
                double dy = y - meanChainlinkReturn;
                meanBinanceReturn += DECAY * dx;
                meanChainlinkReturn += DECAY * dy;
                covariance = (1.0 - DECAY) * covariance + DECAY * dx * dy;
                variance = (1.0 - DECAY) * variance + DECAY * dx * dx;
                samples++;
            }
        }
        previousBinance = binance;
        previousChainlink = chainlink;
        return snapshot();
    }

    public synchronized BigDecimal project(BigDecimal currentChainlinkPrice, BigDecimal binanceReturnRatio) {
        if (!valid(currentChainlinkPrice) || !valid(binanceReturnRatio)) return null;
        double beta = snapshot().beta();
        double projection = currentChainlinkPrice.doubleValue() * Math.exp(beta * Math.log(binanceReturnRatio.doubleValue()));
        return Double.isFinite(projection) && projection > 0.0 ? BigDecimal.valueOf(projection) : null;
    }

    private Calibration snapshot() {
        double beta = variance > MIN_VARIANCE ? covariance / variance : 1.0;
        return new Calibration(samples, Math.clamp(beta, MIN_BETA, MAX_BETA));
    }

    private boolean valid(BigDecimal price) {
        return price != null && price.signum() > 0 && Double.isFinite(price.doubleValue());
    }

    public record Calibration(int samples, double beta) {}
}
