package al.r1.polytrader.services.model;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Queue;

@Getter
@Component
public class Prices {
    BigDecimal avgPrice;
    BigDecimal avg60sPrice;

    Integer activeAvgProviders = 0;
    Integer active60sAvgProviders = 0;

    BigDecimal polymarket60sAvgPrice;

    BigDecimal binancePrice;
    BigDecimal binanceAvg60sPrice;

    Queue<BigDecimal> binancePricesQueue = new ArrayDeque<>();

    Boolean binanceInAvgPrice = false;
    Boolean binanceIn60sAvgPrice = false;


    //TODO implement later more price providers
    BigDecimal coinbasePrice;
    Queue<BigDecimal> coinbasePricesQueue;

    BigDecimal krakenPrice;
    Queue<BigDecimal> krakenPricesQueue;

    BigDecimal bybitPrice;
    Queue<BigDecimal> bybitPricesQueue;

    BigDecimal okxPrice;
    Queue<BigDecimal> okxPricesQueue;

    public synchronized void setPolymarket60sAvgPrice(BigDecimal polymarket60sAvgPrice) {
        this.polymarket60sAvgPrice = polymarket60sAvgPrice;
    }

    public synchronized void setBinancePrice(BigDecimal newBinancePrice) {
        updateAvgPrice(binancePrice, newBinancePrice, PriceProviders.BINANCE);
        this.binancePrice = newBinancePrice;
        binancePricesQueue.add(binancePrice);

        if (binancePricesQueue.size() > 60) {
            BigDecimal removedPrice = binancePricesQueue.remove();
            BigDecimal oldBinane60sAvg = binanceAvg60sPrice;

            binanceAvg60sPrice = binanceAvg60sPrice
                    .multiply(BigDecimal.valueOf(60))
                    .add(binancePrice)
                    .subtract(removedPrice)
                    .divide(BigDecimal.valueOf(60), 2, RoundingMode.HALF_UP);

            updateAvg60sPrice(oldBinane60sAvg, binanceAvg60sPrice, PriceProviders.BINANCE);
        }

    }

    private void updateAvg60sPrice(BigDecimal oldPrice, BigDecimal newPrice, PriceProviders source) {
        if (source.equals(PriceProviders.BINANCE)) {
            if (binanceIn60sAvgPrice) {
                this.avg60sPrice = avg60sPrice
                        .multiply(BigDecimal.valueOf(active60sAvgProviders))
                        .add(newPrice)
                        .subtract(oldPrice)
                        .divide(BigDecimal.valueOf(active60sAvgProviders), 2, RoundingMode.HALF_UP);
            } else if (active60sAvgProviders > 0) {
                active60sAvgProviders += 1;
                binanceIn60sAvgPrice = true;
                this.avg60sPrice = avg60sPrice
                        .add(newPrice)
                        .divide(BigDecimal.valueOf(active60sAvgProviders), 2, RoundingMode.HALF_UP);
            } else {
                active60sAvgProviders++;
                binanceIn60sAvgPrice = true;
                this.avg60sPrice = newPrice;
            }
        }
    }

    private void updateAvgPrice(BigDecimal oldPrice, BigDecimal newPrice, PriceProviders source) {
        if (source.equals(PriceProviders.BINANCE)) {
            if (binanceInAvgPrice) {
                this.avgPrice = avgPrice
                        .multiply(BigDecimal.valueOf(activeAvgProviders))
                        .add(newPrice)
                        .subtract(oldPrice)
                        .divide(BigDecimal.valueOf(activeAvgProviders), 2, RoundingMode.HALF_UP);
            } else if (activeAvgProviders > 0) {
                activeAvgProviders += 1;
                binanceInAvgPrice = true;
                this.avgPrice = avgPrice
                        .add(newPrice)
                        .divide(BigDecimal.valueOf(activeAvgProviders), 2, RoundingMode.HALF_UP);
            } else {
                activeAvgProviders++;
                binanceInAvgPrice = true;
                this.avgPrice = newPrice;
            }
        }
    }
}
