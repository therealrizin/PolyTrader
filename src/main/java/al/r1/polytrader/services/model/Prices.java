package al.r1.polytrader.services.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class Prices {

    /*
     * Keep substantially more than one 5-minute market.
     *
     * This is important because settlement can happen a few seconds after
     * the market closes. We still need the observation from the actual
     * Polymarket/Chainlink close time.
     */
    private static final long RAW_HISTORY_RETENTION_MILLIS =
            15 * 60 * 1000L;

    private static final int HISTORY_SIZE = 60;

    private record SymbolState(
            AtomicReference<BigDecimal> price,
            AtomicReference<BigDecimal> avg60sPrice,
            Deque<PriceObservation> rawHistory,
            Deque<PricePoint> history
    ) {
        static SymbolState empty() {
            return new SymbolState(
                    new AtomicReference<>(),
                    new AtomicReference<>(),
                    new ArrayDeque<>(),
                    new ArrayDeque<>()
            );
        }
    }

    /**
     * Exact raw Chainlink observation received from Polymarket RTDS.
     *
     * observedAtMillis is the timestamp supplied by the RTDS payload,
     * NOT the local time at which our application happened to receive it.
     */
    public record PriceObservation(
            long observedAtMillis,
            BigDecimal price
    ) {}

    private final Map<ChainlinkSymbol, SymbolState> states =
            new EnumMap<>(ChainlinkSymbol.class);

    /*
     * Raw per-exchange spot prices.
     *
     * These are informational only and are NOT used as the canonical
     * Polymarket/Chainlink reference.
     */
    private final Map<PriceProviders, BigDecimal> providerPrices =
            new EnumMap<>(PriceProviders.class);

    public Prices() {
        for (ChainlinkSymbol symbol : ChainlinkSymbol.values()) {
            states.put(symbol, SymbolState.empty());
        }
    }

    /**
     * Updates the current raw price without timestamped history.
     *
     * Kept for compatibility with existing callers.
     */
    public void updatePrice(ChainlinkSymbol symbol, BigDecimal price) {
        if (symbol == null || price == null) {
            return;
        }

        states.get(symbol).price().set(price);
    }

    /**
     * Updates the current raw price and stores the exact RTDS observation
     * timestamp.
     *
     * This is the method ChainlinkPriceStreamClient should use.
     */
    public synchronized void updatePrice(
            ChainlinkSymbol symbol,
            BigDecimal price,
            long observedAtMillis
    ) {
        if (symbol == null || price == null || observedAtMillis <= 0) {
            return;
        }

        SymbolState state = states.get(symbol);

        state.price().set(price);

        state.rawHistory().addLast(
                new PriceObservation(observedAtMillis, price)
        );

        long cutoff = observedAtMillis - RAW_HISTORY_RETENTION_MILLIS;

        while (!state.rawHistory().isEmpty()
                && state.rawHistory().peekFirst().observedAtMillis() < cutoff) {

            state.rawHistory().pollFirst();
        }
    }

    public void updateAvg60sPrice(
            ChainlinkSymbol symbol,
            BigDecimal avg60sPrice
    ) {
        if (symbol == null || avg60sPrice == null) {
            return;
        }

        states.get(symbol).avg60sPrice().set(avg60sPrice);
    }

    public BigDecimal getPrice(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        return state == null ? null : state.price().get();
    }

    public BigDecimal getAvg60sPrice(ChainlinkSymbol symbol) {
        SymbolState state = states.get(symbol);
        return state == null ? null : state.avg60sPrice().get();
    }

    /**
     * Returns the latest raw Chainlink observation whose timestamp is
     * <= targetTimestampMillis.
     *
     * This is exactly what we want when resolving a finished market:
     *
     *   "What was the last Polymarket Chainlink price at/before close?"
     *
     * We intentionally do NOT return the latest observation if it happened
     * after the target timestamp.
     */
    public synchronized PriceObservation getRawPriceAtOrBefore(
            ChainlinkSymbol symbol,
            long targetTimestampMillis
    ) {
        SymbolState state = states.get(symbol);

        if (state == null || state.rawHistory().isEmpty()) {
            return null;
        }

        PriceObservation result = null;

        for (PriceObservation observation : state.rawHistory()) {
            if (observation.observedAtMillis() > targetTimestampMillis) {
                break;
            }

            result = observation;
        }

        return result;
    }

    /**
     * Returns the raw observation closest to the requested timestamp.
     *
     * Normally settlement should use getRawPriceAtOrBefore().
     */
    public synchronized PriceObservation getClosestRawPrice(
            ChainlinkSymbol symbol,
            long targetTimestampMillis
    ) {
        SymbolState state = states.get(symbol);

        if (state == null || state.rawHistory().isEmpty()) {
            return null;
        }

        PriceObservation closest = null;
        long closestDistance = Long.MAX_VALUE;

        for (PriceObservation observation : state.rawHistory()) {
            long distance = Math.abs(
                    observation.observedAtMillis() - targetTimestampMillis
            );

            if (distance < closestDistance) {
                closest = observation;
                closestDistance = distance;
            }
        }

        return closest;
    }

    public synchronized List<PriceObservation> getRawHistory(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return List.of();
        }

        return new ArrayList<>(state.rawHistory());
    }

    public synchronized void recordSnapshot(
            ChainlinkSymbol symbol,
            LocalDateTime at
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return;
        }

        state.history().addLast(
                new PricePoint(
                        at,
                        state.price().get(),
                        state.avg60sPrice().get()
                )
        );

        while (state.history().size() > HISTORY_SIZE) {
            state.history().pollFirst();
        }
    }

    public synchronized List<PricePoint> getRecentHistory(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return List.of();
        }

        return new ArrayList<>(state.history());
    }

    // ------------------------------------------------------------------
    // Raw per-exchange spot prices
    // ------------------------------------------------------------------

    public void setProviderPrice(
            PriceProviders provider,
            BigDecimal price
    ) {
        if (provider == null || price == null) {
            return;
        }

        providerPrices.put(provider, price);
    }

    public void setBinancePrice(BigDecimal price) {
        setProviderPrice(PriceProviders.BINANCE, price);
    }

    public void setPolymarketPrice(BigDecimal price) {
        setProviderPrice(PriceProviders.POLYMARKET, price);
    }

    public BigDecimal getProviderPrice(PriceProviders provider) {
        return providerPrices.get(provider);
    }
}