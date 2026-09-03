package al.r1.polytrader.services.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
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

    /*
     * A price is considered usable for a REAL BUY only if our application
     * received/processes the RTDS price no more than this long ago.
     *
     * 500 ms = 0.5 seconds.
     */
    public static final long MAX_PRICE_AGE_MILLIS = 500L;

    /*
     * Protect against a broken clock / malformed RTDS timestamp.
     *
     * We allow a very small amount of clock skew, but a timestamp more
     * than this into the future is not considered valid.
     */
    private static final long MAX_FUTURE_SKEW_MILLIS = 1_000L;

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
     * IMPORTANT:
     *
     * This method intentionally does NOT update the RTDS timestamp or
     * local freshness timestamp.
     *
     * Therefore a caller using this legacy method can never make an
     * otherwise stale price appear fresh enough for real betting.
     */
    public void updatePrice(
            ChainlinkSymbol symbol,
            BigDecimal price
    ) {
        if (symbol == null || price == null) {
            return;
        }

        SymbolState state = states.get(symbol);

        if (state == null) {
            return;
        }

        state.price().set(price);
    }

    /**
     * Updates the current raw price and stores the exact RTDS observation
     * timestamp.
     *
     * The RTDS timestamp is preserved for historical purposes.
     *
     * Freshness is based on the LOCAL timestamp recorded when this method
     * processes the RTDS price, because RTDS timestamps have only
     * second-level precision in the received payload.
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

        if (state == null) {
            return;
        }

        /*
         * This is the timestamp that matters for the 500 ms freshness
         * requirement.
         *
         * Do NOT use observedAtMillis here.
         */
        long receivedAtMillis =
                System.currentTimeMillis();

        state.price().set(price);

        /*
         * Keep the provider/RTDS timestamp separately.
         */
        state.lastObservedAtMillis().set(
                observedAtMillis
        );

        /*
         * Keep the local receipt/processing timestamp separately.
         */
        state.lastPriceReceivedAtMillis().set(
                receivedAtMillis
        );

        state.rawHistory().addLast(
                new PriceObservation(
                        observedAtMillis,
                        price
                )
        );

        long cutoff =
                observedAtMillis
                        - RAW_HISTORY_RETENTION_MILLIS;

        while (!state.rawHistory().isEmpty()
                && state.rawHistory()
                .peekFirst()
                .observedAtMillis() < cutoff) {

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

        SymbolState state = states.get(symbol);

        if (state == null) {
            return;
        }

        state.avg60sPrice().set(avg60sPrice);
    }

    public BigDecimal getPrice(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        return state == null
                ? null
                : state.price().get();
    }

    public BigDecimal getAvg60sPrice(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        return state == null
                ? null
                : state.avg60sPrice().get();
    }

    /**
     * Returns the timestamp of the latest RTDS observation.
     *
     * This is the PROVIDER timestamp and is intentionally NOT used
     * for the 500 ms freshness check.
     *
     * Returns 0 when no timestamped RTDS price has ever been received.
     */
    public synchronized long getLastPriceTimestampMillis(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return 0L;
        }

        return state.lastObservedAtMillis().get();
    }

    /**
     * Returns the LOCAL timestamp at which our application received/
     * processed the latest timestamped RTDS raw price.
     *
     * This timestamp is used for real-order freshness checks.
     */
    public synchronized long getLastPriceReceivedTimestampMillis(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return 0L;
        }

        return state.lastPriceReceivedAtMillis().get();
    }

    /**
     * Returns the age of the latest RTDS price based on when our
     * application received/processed it.
     *
     * This deliberately does NOT use the RTDS payload timestamp because
     * that timestamp has second-level precision.
     *
     * Returns Long.MAX_VALUE if no timestamped price exists.
     */
    public synchronized long getPriceAgeMillis(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return Long.MAX_VALUE;
        }

        long receivedAtMillis =
                state.lastPriceReceivedAtMillis().get();

        if (receivedAtMillis <= 0) {
            return Long.MAX_VALUE;
        }

        return Math.max(
                0L,
                System.currentTimeMillis()
                        - receivedAtMillis
        );
    }

    /**
     * Returns true only when:
     *
     *   1. A timestamped RTDS price exists.
     *   2. The RTDS observation timestamp is not too far in the future.
     *   3. Our application received/processed the price no more than
     *      500 ms ago.
     *
     * This is intended for REAL order submission.
     */
    public synchronized boolean isPriceFresh(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null) {
            return false;
        }

        long observedAtMillis =
                state.lastObservedAtMillis().get();

        long receivedAtMillis =
                state.lastPriceReceivedAtMillis().get();

        if (observedAtMillis <= 0
                || receivedAtMillis <= 0) {

            return false;
        }

        long now =
                System.currentTimeMillis();

        /*
         * Reject obviously invalid future RTDS timestamps.
         *
         * This check is only for timestamp validity.
         * Freshness itself is checked using receivedAtMillis below.
         */
        if (observedAtMillis
                > now + MAX_FUTURE_SKEW_MILLIS) {

            return false;
        }

        /*
         * ACTUAL REAL-TRADING SAFETY CHECK:
         *
         * The price must have been received by OUR APPLICATION
         * within the last 500 ms.
         */
        long age =
                now - receivedAtMillis;

        return age >= 0
                && age <= MAX_PRICE_AGE_MILLIS;
    }

    /**
     * Returns the latest timestamped RTDS observation.
     */
    public synchronized PriceObservation getLatestRawPrice(
            ChainlinkSymbol symbol
    ) {
        SymbolState state = states.get(symbol);

        if (state == null
                || state.rawHistory().isEmpty()) {

            return null;
        }

        return state.rawHistory().peekLast();
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

        if (state == null
                || state.rawHistory().isEmpty()) {

            return null;
        }

        PriceObservation result = null;

        for (PriceObservation observation :
                state.rawHistory()) {

            if (observation.observedAtMillis()
                    > targetTimestampMillis) {

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

        if (state == null
                || state.rawHistory().isEmpty()) {

            return null;
        }

        PriceObservation closest = null;
        long closestDistance = Long.MAX_VALUE;

        for (PriceObservation observation :
                state.rawHistory()) {

            long distance =
                    Math.abs(
                            observation.observedAtMillis()
                                    - targetTimestampMillis
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

        return new ArrayList<>(
                state.rawHistory()
        );
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

        return new ArrayList<>(
                state.history()
        );
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

        providerPrices.put(
                provider,
                price
        );
    }

    public void setBinancePrice(
            BigDecimal price
    ) {
        setProviderPrice(
                PriceProviders.BINANCE,
                price
        );
    }

    public void setPolymarketPrice(
            BigDecimal price
    ) {
        setProviderPrice(
                PriceProviders.POLYMARKET,
                price
        );
    }

    public BigDecimal getProviderPrice(
            PriceProviders provider
    ) {
        return providerPrices.get(provider);
    }
}
