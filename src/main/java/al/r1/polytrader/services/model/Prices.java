package al.r1.polytrader.services.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class Prices {

    private static final long RAW_HISTORY_RETENTION_MILLIS = 15 * 60 * 1000L;
    private static final int HISTORY_SIZE = 60;
    public static final long MAX_PRICE_AGE_MILLIS = 500L;
    private static final long MAX_FUTURE_SKEW_MILLIS = 1_000L;

    private record PriceObservation(long observedAtMillis, BigDecimal price) {}
    private record PricePoint(LocalDateTime timestamp, BigDecimal price) {}

    private record SymbolState(
            AtomicReference<BigDecimal> price,
            AtomicReference<BigDecimal> avg60sPrice,
            AtomicLong lastObservedAtMillis,
            AtomicLong lastPriceReceivedAtMillis,
            Deque<PriceObservation> rawHistory,
            Deque<PricePoint> history
    ) {
        static SymbolState empty() {
            return new SymbolState(
                    new AtomicReference<>(),
                    new AtomicReference<>(),
                    new AtomicLong(0L),
                    new AtomicLong(0L),
                    new ArrayDeque<>(),
                    new ArrayDeque<>()
            );
        }
    }

    private final Map<ChainlinkSymbol, SymbolState> states = new EnumMap<>(ChainlinkSymbol.class);
    private final Map<PriceProviders, BigDecimal> providerPrices = new EnumMap<>(PriceProviders.class);

    public Prices() {
        for (ChainlinkSymbol symbol : ChainlinkSymbol.values()) {
            states.put(symbol, SymbolState.empty());
        }
    }

    public synchronized void updatePrice(ChainlinkSymbol symbol, BigDecimal price, long observedAtMillis) {
        if (symbol == null || price == null || observedAtMillis <= 0) return;

        SymbolState state = states.get(symbol);
        if (state == null) return;

        long receivedAtMillis = System.currentTimeMillis();
        state.price().set(price);
        state.lastObservedAtMillis().set(observedAtMillis);
        state.lastPriceReceivedAtMillis().set(receivedAtMillis);

        state.rawHistory().addLast(new PriceObservation(observedAtMillis, price));
        long cutoff = observedAtMillis - RAW_HISTORY_RETENTION_MILLIS;
        while (!state.rawHistory().isEmpty() && state.rawHistory().peekFirst().observedAtMillis() < cutoff) {
            state.rawHistory().pollFirst();
        }
    }

    public synchronized void recordSnapshot(ChainlinkSymbol symbol, LocalDateTime timestamp) {
        if (symbol == null || timestamp == null) return;

        SymbolState state = states.get(symbol);
        if (state == null) return;

        BigDecimal currentPrice = state.price().get();
        if (currentPrice == null) return;

        Deque<PricePoint> history = state.history();
        history.addLast(new PricePoint(timestamp, currentPrice));
        while (history.size() > HISTORY_SIZE) {
            history.pollFirst();
        }

        if (!history.isEmpty()) {
            BigDecimal sum = BigDecimal.ZERO;
            for (PricePoint pp : history) {
                sum = sum.add(pp.price());
            }
            BigDecimal avg = sum.divide(BigDecimal.valueOf(history.size()), 10, RoundingMode.HALF_UP);
            state.avg60sPrice().set(avg);
        }
    }

    public void updateAvg60sPrice(ChainlinkSymbol symbol, BigDecimal avg60sPrice) {
        if (symbol == null || avg60sPrice == null) return;
        SymbolState state = states.get(symbol);
        if (state == null) return;
        state.avg60sPrice().set(avg60sPrice);
    }

    public BigDecimal getPrice(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        return state == null ? null : state.price().get();
    }

    public BigDecimal getAvg60sPrice(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        return state == null ? null : state.avg60sPrice().get();
    }

    public synchronized long getLastPriceTimestampMillis(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        return state == null ? 0L : state.lastObservedAtMillis().get();
    }

    public synchronized long getPriceAgeMillis(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) return Long.MAX_VALUE;
        long receivedAtMillis = state.lastPriceReceivedAtMillis().get();
        if (receivedAtMillis <= 0) return Long.MAX_VALUE;
        return Math.max(0L, System.currentTimeMillis() - receivedAtMillis);
    }

    public synchronized boolean isPriceFresh(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) return false;

        long observedAtMillis = state.lastObservedAtMillis().get();
        long receivedAtMillis = state.lastPriceReceivedAtMillis().get();
        if (observedAtMillis <= 0 || receivedAtMillis <= 0) return false;

        long now = System.currentTimeMillis();
        if (observedAtMillis > now + MAX_FUTURE_SKEW_MILLIS) return false;
        long age = now - receivedAtMillis;
        return age >= 0 && age <= MAX_PRICE_AGE_MILLIS;
    }

    public synchronized List<PricePoint> getRecentHistory(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        if (state == null) return List.of();
        return new ArrayList<>(state.history());
    }

    public void setProviderPrice(PriceProviders provider, BigDecimal price) {
        if (provider == null || price == null) return;
        providerPrices.put(provider, price);
    }

    public void setBinancePrice(BigDecimal price) {
        setProviderPrice(PriceProviders.BINANCE, price);
    }

    public void setPolymarketPrice(BigDecimal price) {
        setProviderPrice(PriceProviders.POLYMARKET, price);
    }
}