package al.r1.polytrader.engine;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.MarketSide;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
public class TradingEngine {

    private static final int MAX_MARKET_SECONDS = 300;
    private static final int TWAP_WINDOW_SECONDS = 60;

    private final ProbabilityTable table;
    private final TradingProperties tradingProperties;

    public TradingEngine(ProbabilityTable table, TradingProperties tradingProperties) {
        this.table = table;
        this.tradingProperties = tradingProperties;
    }

    public EvEstimate estimatePricesToMeetEv(BigDecimal currentLivePrice, BigDecimal currentTwapPrice, BigDecimal resolutionPrice, int secondsLeft) {
        int safeSecondsLeft = Math.clamp(secondsLeft, 0, MAX_MARKET_SECONDS);
        double upChance = estimatedUpChance(currentLivePrice, currentTwapPrice, resolutionPrice, safeSecondsLeft);
        double downChance = 1.0 - upChance;
        double upEvRequired = requiredEv(upChance);
        double downEvRequired = requiredEv(downChance);
        double upPriceToMeetEv = maxBuyPriceForEv(upChance, upEvRequired);
        double downPriceToMeetEv = maxBuyPriceForEv(downChance, downEvRequired);
        return new EvEstimate(upChance, upPriceToMeetEv, upEvRequired, downChance, downPriceToMeetEv, downEvRequired);
    }

    public double maxBuyPriceForEv(double winChance, double targetEv) {
        if (!Double.isFinite(winChance) || !Double.isFinite(targetEv) || winChance <= 0.0 || winChance > 1.0) {
            return 0.0;
        }
        double maxPrice = calculateMaxBuyPrice(winChance, targetEv);
        if (!Double.isFinite(maxPrice)) {
            return 0.0;
        }
        return Math.clamp(maxPrice, 0.0, 1.0);
    }

    public double netSellValuePerShare(double sellPrice) {
        if (!Double.isFinite(sellPrice) || sellPrice <= 0.0 || sellPrice > 1.0) {
            return 0.0;
        }
        double netValue = sellPrice - takerFeeEstimate(sellPrice);
        if (!Double.isFinite(netValue)) {
            return 0.0;
        }
        return Math.clamp(netValue, 0.0, 1.0);
    }

    public double requiredEv(double winChance) {
        double minEv = tradingProperties.minimumExpectedEv();
        double minWinChance = tradingProperties.minimumWinChance();

        if (winChance <= minWinChance) {
            return minEv;
        }
        if (winChance >= 0.90) {
            return minEv / 5.0;
        }
        double progress = (winChance - minWinChance) / (0.90 - minWinChance);
        return minEv * (1.0 - progress * 0.8);
    }

    public double minSellPriceForNetValue(double targetNetValue) {
        if (!Double.isFinite(targetNetValue) || targetNetValue <= 0.0) {
            return 1.0;
        }
        double price = Math.clamp(targetNetValue, 0.0, 1.0);
        for (int i = 0; i < 20; i++) {
            double takerFee = takerFeeEstimate(price);
            double newPrice = targetNetValue + takerFee;
            if (!Double.isFinite(newPrice)) {
                return 1.0;
            }
            newPrice = Math.clamp(newPrice, 0.0, 1.0);
            if (Math.abs(newPrice - price) < 1e-10) {
                return newPrice;
            }
            price = newPrice;
        }
        return price;
    }

    public double realizedBuyEv(double winChance, double price) {
        if (!Double.isFinite(winChance) || winChance <= 0.0 || winChance > 1.0
                || !Double.isFinite(price) || price <= 0.0 || price > 1.0) {
            return 0.0;
        }
        double takerFee = takerFeeEstimate(price);
        double ev = winChance * takerFee + winChance * (1.0 - takerFee) / price - 1.0;
        return Double.isFinite(ev) ? ev : 0.0;
    }

    private double calculateMaxBuyPrice(double winChance, double targetEv) {
        double price = winChance;
        for (int i = 0; i < 20; i++) {
            double takerFee = takerFeeEstimate(price);
            double denominator = ((1.0 + targetEv) / winChance) - takerFee;
            if (!Double.isFinite(denominator) || denominator <= 0.0) {
                return 0.0;
            }
            double newPrice = (1.0 - takerFee) / denominator;
            if (!Double.isFinite(newPrice)) {
                return 0.0;
            }
            newPrice = Math.clamp(newPrice, 0.0, 1.0);
            if (Math.abs(newPrice - price) < 1e-10) {
                return newPrice;
            }
            price = newPrice;
        }
        return price;
    }

    private double takerFeeEstimate(double price) {
        double feeCoefficient = tradingProperties.takerFee();
        if (!Double.isFinite(feeCoefficient) || feeCoefficient < 0.0 || feeCoefficient >= 1.0) {
            return 0.0;
        }
        price = Math.clamp(price, 0.0, 1.0);
        return feeCoefficient * price * (1.0 - price);
    }

    private double estimatedUpChance(BigDecimal currentLivePrice, BigDecimal currentTwapPrice, BigDecimal referencePrice, int secondsLeft) {
        if (currentLivePrice == null
                || currentTwapPrice == null
                || referencePrice == null
                || currentLivePrice.signum() <= 0
                || currentTwapPrice.signum() <= 0
                || referencePrice.signum() <= 0) {
            return -1;
        }

        secondsLeft = Math.clamp(secondsLeft, 0, MAX_MARKET_SECONDS);

        if (secondsLeft >= TWAP_WINDOW_SECONDS) {
            double requiredPctChange = percentageChange(currentLivePrice, referencePrice);
            return probabilityForChange(requiredPctChange, secondsLeft);
        }

        double futureWeight = (double) secondsLeft / TWAP_WINDOW_SECONDS;

        if (futureWeight <= 0.0) {
            return currentTwapPrice.compareTo(referencePrice) > 0 ? 1.0 : 0.0;
        }

        double currentTwap = currentTwapPrice.doubleValue();
        double reference = referencePrice.doubleValue();
        double requiredFutureLivePrice = (reference - currentTwap * (1.0 - futureWeight)) / futureWeight;
        double currentLive = currentLivePrice.doubleValue();
        double requiredPctChange = (requiredFutureLivePrice - currentLive) / currentLive * 100.0;

        return probabilityForChange(requiredPctChange, secondsLeft);
    }

    private double probabilityForChange(double requiredPctChange, int secondsLeft) {
        if (requiredPctChange < 0.0) {
            double probability = table.getChance(secondsLeft, Math.abs(requiredPctChange));
            return clampProbability(1.0 - probability);
        }
        double probability = table.getChance(secondsLeft, requiredPctChange);
        return clampProbability(probability);
    }

    private double percentageChange(BigDecimal from, BigDecimal to) {
        return to.subtract(from).divide(from, 8, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100))
                .doubleValue();
    }

    private double clampProbability(double probability) {
        return Math.clamp(probability, 0.0, 1.0);
    }
}