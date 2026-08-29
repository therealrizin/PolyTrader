package al.r1.polytrader.services.model;

import lombok.Getter;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Getter
@Component
public class Prices {

    // Overall averages
    private BigDecimal avgPrice;
    private BigDecimal avg60sPrice;

    // Count of providers contributing to overall averages
    private int activeAvgProviders = 0;
    private int active60sAvgProviders = 0;

    // Polymarket specific (not part of averages)
    private BigDecimal polymarketPrice;
    private final Deque<PolymarketSample> polymarketRecentHistory = new ArrayDeque<>();

    // Binance
    private BigDecimal binancePrice;
    private BigDecimal binanceAvg60sPrice;
    private final Queue<BigDecimal> binancePricesQueue = new ArrayDeque<>();
    private boolean binanceInAvgPrice = false;
    private boolean binanceIn60sAvgPrice = false;

    // Coinbase
    private BigDecimal coinbasePrice;
    private BigDecimal coinbaseAvg60sPrice;
    private final Queue<BigDecimal> coinbasePricesQueue = new ArrayDeque<>();
    private boolean coinbaseInAvgPrice = false;
    private boolean coinbaseIn60sAvgPrice = false;

    // Kraken
    private BigDecimal krakenPrice;
    private BigDecimal krakenAvg60sPrice;
    private final Queue<BigDecimal> krakenPricesQueue = new ArrayDeque<>();
    private boolean krakenInAvgPrice = false;
    private boolean krakenIn60sAvgPrice = false;

    // Bybit
    private BigDecimal bybitPrice;
    private BigDecimal bybitAvg60sPrice;
    private final Queue<BigDecimal> bybitPricesQueue = new ArrayDeque<>();
    private boolean bybitInAvgPrice = false;
    private boolean bybitIn60sAvgPrice = false;

    // OKX
    private BigDecimal okxPrice;
    private BigDecimal okxAvg60sPrice;
    private final Queue<BigDecimal> okxPricesQueue = new ArrayDeque<>();
    private boolean okxInAvgPrice = false;
    private boolean okxIn60sAvgPrice = false;

    // ------------------------------------------------------------------------
    // Polymarket
    // ------------------------------------------------------------------------

    public synchronized void setPolymarketPrice(BigDecimal polymarketPrice) {
        this.polymarketPrice = polymarketPrice;
    }

    /**
     * Records one price snapshot per live-data tick. Keeping a count rather
     * than a time cutoff guarantees that the API exposes exactly the latest
     * ten one-second samples once the service has warmed up.
     *
     * Paired against avg60sPrice, not the instantaneous avgPrice: Polymarket's
     * polymarketPrice is itself a 60s TWAP, so comparing it to an unwindowed
     * average would be comparing two different smoothing bases.
     */
    public synchronized void recordPriceSnapshot(long timestampMillis) {
        polymarketRecentHistory.addLast(new PolymarketSample(timestampMillis, polymarketPrice, avg60sPrice));
        while (polymarketRecentHistory.size() > 10) {
            polymarketRecentHistory.pollFirst();
        }
    }

    public synchronized List<PolymarketSample> getPolymarketRecentHistory() {
        return new ArrayList<>(polymarketRecentHistory);
    }

    // ------------------------------------------------------------------------
    // Binance
    // ------------------------------------------------------------------------
    public synchronized void setBinancePrice(BigDecimal newBinancePrice) {
        if (newBinancePrice == null) {
            return; // or log warning
        }

        BigDecimal oldPrice = this.binancePrice;

        // Update overall avgPrice
        updateOverallAvgPrice(oldPrice, newBinancePrice,
                this.binanceInAvgPrice, () -> this.activeAvgProviders,
                (newInFlag, newCount) -> {
                    this.binanceInAvgPrice = newInFlag;
                    this.activeAvgProviders = newCount;
                },
                () -> this.avgPrice,
                (newAvg) -> this.avgPrice = newAvg);

        // Update provider price
        this.binancePrice = newBinancePrice;

        // Update 60s average
        updateProvider60sAverage(this.binancePricesQueue, newBinancePrice,
                this.binanceAvg60sPrice,
                this.binanceIn60sAvgPrice,
                () -> this.active60sAvgProviders,
                (newInFlag, newCount) -> {
                    this.binanceIn60sAvgPrice = newInFlag;
                    this.active60sAvgProviders = newCount;
                },
                () -> this.binanceAvg60sPrice,
                (newAvg) -> this.binanceAvg60sPrice = newAvg,
                () -> this.avg60sPrice,
                (newOverallAvg) -> this.avg60sPrice = newOverallAvg);
    }

    // ------------------------------------------------------------------------
    // Coinbase
    // ------------------------------------------------------------------------
    public synchronized void setCoinbasePrice(BigDecimal newCoinbasePrice) {
        if (newCoinbasePrice == null) {
            return;
        }

        BigDecimal oldPrice = this.coinbasePrice;

        updateOverallAvgPrice(oldPrice, newCoinbasePrice,
                this.coinbaseInAvgPrice, () -> this.activeAvgProviders,
                (newInFlag, newCount) -> {
                    this.coinbaseInAvgPrice = newInFlag;
                    this.activeAvgProviders = newCount;
                },
                () -> this.avgPrice,
                (newAvg) -> this.avgPrice = newAvg);

        this.coinbasePrice = newCoinbasePrice;

        updateProvider60sAverage(this.coinbasePricesQueue, newCoinbasePrice,
                this.coinbaseAvg60sPrice,
                this.coinbaseIn60sAvgPrice,
                () -> this.active60sAvgProviders,
                (newInFlag, newCount) -> {
                    this.coinbaseIn60sAvgPrice = newInFlag;
                    this.active60sAvgProviders = newCount;
                },
                () -> this.coinbaseAvg60sPrice,
                (newAvg) -> this.coinbaseAvg60sPrice = newAvg,
                () -> this.avg60sPrice,
                (newOverallAvg) -> this.avg60sPrice = newOverallAvg);
    }

    // ------------------------------------------------------------------------
    // Kraken
    // ------------------------------------------------------------------------
    public synchronized void setKrakenPrice(BigDecimal newKrakenPrice) {
        if (newKrakenPrice == null) {
            return;
        }

        BigDecimal oldPrice = this.krakenPrice;

        updateOverallAvgPrice(oldPrice, newKrakenPrice,
                this.krakenInAvgPrice, () -> this.activeAvgProviders,
                (newInFlag, newCount) -> {
                    this.krakenInAvgPrice = newInFlag;
                    this.activeAvgProviders = newCount;
                },
                () -> this.avgPrice,
                (newAvg) -> this.avgPrice = newAvg);

        this.krakenPrice = newKrakenPrice;

        updateProvider60sAverage(this.krakenPricesQueue, newKrakenPrice,
                this.krakenAvg60sPrice,
                this.krakenIn60sAvgPrice,
                () -> this.active60sAvgProviders,
                (newInFlag, newCount) -> {
                    this.krakenIn60sAvgPrice = newInFlag;
                    this.active60sAvgProviders = newCount;
                },
                () -> this.krakenAvg60sPrice,
                (newAvg) -> this.krakenAvg60sPrice = newAvg,
                () -> this.avg60sPrice,
                (newOverallAvg) -> this.avg60sPrice = newOverallAvg);
    }

    // ------------------------------------------------------------------------
    // Bybit
    // ------------------------------------------------------------------------
    public synchronized void setBybitPrice(BigDecimal newBybitPrice) {
        if (newBybitPrice == null) {
            return;
        }

        BigDecimal oldPrice = this.bybitPrice;

        updateOverallAvgPrice(oldPrice, newBybitPrice,
                this.bybitInAvgPrice, () -> this.activeAvgProviders,
                (newInFlag, newCount) -> {
                    this.bybitInAvgPrice = newInFlag;
                    this.activeAvgProviders = newCount;
                },
                () -> this.avgPrice,
                (newAvg) -> this.avgPrice = newAvg);

        this.bybitPrice = newBybitPrice;

        updateProvider60sAverage(this.bybitPricesQueue, newBybitPrice,
                this.bybitAvg60sPrice,
                this.bybitIn60sAvgPrice,
                () -> this.active60sAvgProviders,
                (newInFlag, newCount) -> {
                    this.bybitIn60sAvgPrice = newInFlag;
                    this.active60sAvgProviders = newCount;
                },
                () -> this.bybitAvg60sPrice,
                (newAvg) -> this.bybitAvg60sPrice = newAvg,
                () -> this.avg60sPrice,
                (newOverallAvg) -> this.avg60sPrice = newOverallAvg);
    }

    // ------------------------------------------------------------------------
    // OKX
    // ------------------------------------------------------------------------
    public synchronized void setOkxPrice(BigDecimal newOkxPrice) {
        if (newOkxPrice == null) {
            return;
        }

        BigDecimal oldPrice = this.okxPrice;

        updateOverallAvgPrice(oldPrice, newOkxPrice,
                this.okxInAvgPrice, () -> this.activeAvgProviders,
                (newInFlag, newCount) -> {
                    this.okxInAvgPrice = newInFlag;
                    this.activeAvgProviders = newCount;
                },
                () -> this.avgPrice,
                (newAvg) -> this.avgPrice = newAvg);

        this.okxPrice = newOkxPrice;

        updateProvider60sAverage(this.okxPricesQueue, newOkxPrice,
                this.okxAvg60sPrice,
                this.okxIn60sAvgPrice,
                () -> this.active60sAvgProviders,
                (newInFlag, newCount) -> {
                    this.okxIn60sAvgPrice = newInFlag;
                    this.active60sAvgProviders = newCount;
                },
                () -> this.okxAvg60sPrice,
                (newAvg) -> this.okxAvg60sPrice = newAvg,
                () -> this.avg60sPrice,
                (newOverallAvg) -> this.avg60sPrice = newOverallAvg);
    }

    // ========================================================================
    // Private helpers for overall avgPrice
    // ========================================================================
    private void updateOverallAvgPrice(BigDecimal oldPrice, BigDecimal newPrice,
                                       boolean alreadyIncluded,
                                       IntSupplier currentCountSupplier,
                                       BiIntConsumer inclusionUpdater,
                                       Supplier<BigDecimal> currentAvgSupplier,
                                       Consumer<BigDecimal> avgUpdater) {
        int currentCount = currentCountSupplier.getAsInt();

        if (alreadyIncluded) {
            // Update existing provider – correct formula
            if (currentCount > 0) {
                BigDecimal newAvg = currentAvgSupplier.get()
                        .multiply(BigDecimal.valueOf(currentCount))
                        .add(newPrice)
                        .subtract(oldPrice)
                        .divide(BigDecimal.valueOf(currentCount), 2, RoundingMode.HALF_UP);
                avgUpdater.accept(newAvg);
            }
            // if count == 0 shouldn't happen, but we keep as is
        } else {
            // Add new provider
            if (currentCount == 0) {
                avgUpdater.accept(newPrice);
            } else {
                BigDecimal newAvg = currentAvgSupplier.get()
                        .multiply(BigDecimal.valueOf(currentCount))
                        .add(newPrice)
                        .divide(BigDecimal.valueOf(currentCount + 1), 2, RoundingMode.HALF_UP);
                avgUpdater.accept(newAvg);
            }
            // Mark as included and increment count
            inclusionUpdater.accept(true, currentCount + 1);
        }
    }

    // ========================================================================
    // Private helpers for per‑provider 60s average and overall avg60sPrice
    // ========================================================================
    private void updateProvider60sAverage(Queue<BigDecimal> queue,
                                          BigDecimal newPrice,
                                          BigDecimal currentProviderAvg60s,
                                          boolean alreadyIncluded60s,
                                          IntSupplier current60sCountSupplier,
                                          BiIntConsumer inclusion60sUpdater,
                                          Supplier<BigDecimal> providerAvg60sSupplier,
                                          Consumer<BigDecimal> providerAvg60sUpdater,
                                          Supplier<BigDecimal> overallAvg60sSupplier,
                                          Consumer<BigDecimal> overallAvg60sUpdater) {

        // Add new price to the queue
        queue.add(newPrice);

        // If we have more than 60, remove the oldest
        BigDecimal removed = null;
        if (queue.size() > 60) {
            removed = queue.remove();
        }

        int currentSize = queue.size();

        // Compute or update the provider's 60s average
        BigDecimal newProviderAvg60s = null;

        if (currentSize == 60) {
            // We have exactly 60 prices – compute from scratch if not yet set,
            // or use rolling formula if already set (only if we removed an element)
            if (currentProviderAvg60s == null) {
                // First time we have 60 – compute sum
                BigDecimal sum = queue.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
                newProviderAvg60s = sum.divide(BigDecimal.valueOf(60), 2, RoundingMode.HALF_UP);
            } else {
                // Rolling update: oldAvg * 60 + newPrice - removed (if we removed one)
                if (removed != null) {
                    newProviderAvg60s = currentProviderAvg60s
                            .multiply(BigDecimal.valueOf(60))
                            .add(newPrice)
                            .subtract(removed)
                            .divide(BigDecimal.valueOf(60), 2, RoundingMode.HALF_UP);
                } else {
                    newProviderAvg60s = currentProviderAvg60s;
                }
            }
        } else if (currentSize < 60) {
            // Not enough prices yet – no average
            return;
        }

        // If we have a new provider average, update the overall avg60sPrice
        if (newProviderAvg60s != null) {
            // Update overall avg60sPrice using similar logic to avgPrice
            BigDecimal oldOverallAvg60s = overallAvg60sSupplier.get();
            BigDecimal newOverallAvg60s;

            int current60sCount = current60sCountSupplier.getAsInt();

            if (alreadyIncluded60s) {
                // Update existing provider's 60s average in the overall average
                // oldProviderAvg60s is the previous value for this provider, which we already have as currentProviderAvg60s
                BigDecimal oldProviderAvg60s = currentProviderAvg60s; // before update
                if (current60sCount > 0) {
                    newOverallAvg60s = oldOverallAvg60s
                            .multiply(BigDecimal.valueOf(current60sCount))
                            .add(newProviderAvg60s)
                            .subtract(oldProviderAvg60s)
                            .divide(BigDecimal.valueOf(current60sCount), 2, RoundingMode.HALF_UP);
                    overallAvg60sUpdater.accept(newOverallAvg60s);
                }
                // if count == 0, shouldn't happen
            } else {
                // Add new provider's 60s average to the overall average
                if (current60sCount == 0) {
                    newOverallAvg60s = newProviderAvg60s;
                } else {
                    newOverallAvg60s = oldOverallAvg60s
                            .multiply(BigDecimal.valueOf(current60sCount))
                            .add(newProviderAvg60s)
                            .divide(BigDecimal.valueOf(current60sCount + 1), 2, RoundingMode.HALF_UP);
                }
                overallAvg60sUpdater.accept(newOverallAvg60s);
                // Mark as included and increment count
                inclusion60sUpdater.accept(true, current60sCount + 1);
            }

            // Finally, update the provider's stored 60s average
            providerAvg60sUpdater.accept(newProviderAvg60s);
        }
    }

    // ========================================================================
    // Functional interfaces for cleaner code
    // ========================================================================
    @FunctionalInterface
    private interface IntSupplier {
        int getAsInt();
    }

    @FunctionalInterface
    private interface BiIntConsumer {
        void accept(boolean flag, int count);
    }

    @FunctionalInterface
    private interface Supplier<T> {
        T get();
    }

    @FunctionalInterface
    private interface Consumer<T> {
        void accept(T t);
    }
}