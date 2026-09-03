package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides the currently active Polymarket BTC 5-minute market.
 *
 * Gamma is used only for:
 *  - discovering the current market
 *  - obtaining its end time
 *  - obtaining the CLOB token IDs indirectly through the WebSocket client
 *
 * Live UP/DOWN prices come exclusively from the Polymarket CLOB WebSocket.
 */
@Slf4j
@Component
public class PolymarketDataProvider {

    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final String SLUG_PREFIX = "btc-updown-5m-";

    private static final Duration SNAPSHOT_HEARTBEAT_INTERVAL =
            Duration.ofSeconds(30);

    private static final ChainlinkSymbol SYMBOL =
            ChainlinkSymbol.BTC_USD;

    private final WebClient gammaWebClient;
    private final PolymarketProperties properties;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PolymarketClock polymarketClock;
    private final PolymarketMarketWebSocketClient marketWebSocketClient;

    /**
     * Current market slug.
     */
    private final AtomicReference<String> currentSlug =
            new AtomicReference<>();

    /**
     * Reference price captured when the current 5m window starts.
     */
    private final AtomicReference<BigDecimal> referenceOpenPrice =
            new AtomicReference<>();

    /**
     * Current best bid / ask received from the CLOB WebSocket.
     */
    private final AtomicReference<BigDecimal> upBestBid =
            new AtomicReference<>();

    private final AtomicReference<BigDecimal> upBestAsk =
            new AtomicReference<>();

    private final AtomicReference<BigDecimal> downBestBid =
            new AtomicReference<>();

    private final AtomicReference<BigDecimal> downBestAsk =
            new AtomicReference<>();

    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot =
            new AtomicReference<>();

    private final AtomicReference<String> lastLoggedSignature =
            new AtomicReference<>();

    private final AtomicReference<Instant> lastSnapshotHeartbeatAt =
            new AtomicReference<>(Instant.EPOCH);

    public PolymarketDataProvider(
            @Qualifier("gammaWebClient")
            WebClient gammaWebClient,
            PolymarketProperties properties,
            TaskScheduler liveDataTaskScheduler,
            Prices prices,
            PolymarketClock polymarketClock,
            PolymarketMarketWebSocketClient marketWebSocketClient) {

        this.gammaWebClient = gammaWebClient;
        this.properties = properties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.polymarketClock = polymarketClock;
        this.marketWebSocketClient = marketWebSocketClient;
    }

    @PostConstruct
    public void start() {

        if (properties.gammaBaseUrl() == null
                || properties.gammaBaseUrl().isBlank()) {

            throw new IllegalStateException(
                    "services.polymarket.gamma-base-url is not set. " +
                            "Expected something like " +
                            "https://gamma-api.polymarket.com"
            );
        }

        log.info(
                "PolymarketDataProvider starting: gammaBaseUrl={} marketRefreshSeconds={}",
                properties.gammaBaseUrl(),
                properties.marketRefreshSeconds()
        );

        refresh();

        int refreshSeconds =
                properties.marketRefreshSeconds() > 0
                        ? properties.marketRefreshSeconds()
                        : 5;

        liveDataTaskScheduler.scheduleAtFixedRate(
                this::refresh,
                Duration.ofSeconds(refreshSeconds)
        );
    }

    public Optional<PolymarketMarketSnapshot> currentSnapshot() {
        return Optional.ofNullable(latestSnapshot.get());
    }

    /**
     * Called by PolymarketMarketWebSocketClient whenever the live
     * CLOB best bid / ask changes.
     */
    private void onPriceUpdate(
            al.r1.polytrader.engine.model.MarketSide side,
            BigDecimal bestBid,
            BigDecimal bestAsk) {

        String slug = currentSlug.get();

        if (slug == null) {
            return;
        }

        switch (side) {

            case UP -> {
                if (bestBid != null) {
                    upBestBid.set(bestBid);
                }

                if (bestAsk != null) {
                    upBestAsk.set(bestAsk);
                }
            }

            case DOWN -> {
                if (bestBid != null) {
                    downBestBid.set(bestBid);
                }

                if (bestAsk != null) {
                    downBestAsk.set(bestAsk);
                }
            }
        }

        rebuildSnapshot(slug);
    }

    private void refresh() {

        try {

            String slug = computeCurrentSlug();

            JsonNode event = fetchEventBySlug(slug);

            if (event == null) {

                log.info(
                        "REFRESH result=NO_EVENT slug={} " +
                                "detail='Gamma returned no event for this slug'",
                        slug
                );

                latestSnapshot.set(null);
                return;
            }

            JsonNode marketsNode = event.get("markets");

            if (marketsNode == null
                    || !marketsNode.isArray()
                    || marketsNode.isEmpty()) {

                log.warn(
                        "REFRESH result=NO_MARKETS slug={} " +
                                "detail='event found but markets[] missing/empty'",
                        slug
                );

                latestSnapshot.set(null);
                return;
            }

            JsonNode market = marketsNode.get(0);

            long nowMillis = polymarketClock.nowMillis();

            long endEpochMillis = parseEndDate(market, nowMillis);

            long windowOpenEpochMillis =
                    endEpochMillis - WINDOW.toMillis();

            long secondsUntilClose =
                    Math.max(
                            0,
                            (endEpochMillis - nowMillis) / 1000
                    );

            long secondsSinceOpen =
                    Math.max(
                            0,
                            (nowMillis - windowOpenEpochMillis) / 1000
                    );

            handleWindowChange(slug);

            captureReferencePriceIfNeeded(
                    slug,
                    secondsSinceOpen
            );

            /*
             * The CLOB WebSocket is the ONLY source of live UP/DOWN
             * prices now.
             */
            marketWebSocketClient.subscribe(
                    slug,
                    this::onPriceUpdate
            );

            BigDecimal upAsk = upBestAsk.get();
            BigDecimal downAsk = downBestAsk.get();

            BigDecimal upBid = upBestBid.get();
            BigDecimal downBid = downBestBid.get();

            /*
             * We intentionally don't create a snapshot until both
             * sides have received live CLOB data.
             */
            if (upAsk == null || downAsk == null) {

                log.info(
                        "REFRESH result=WAITING_FOR_CLOB_PRICES " +
                                "slug={} upBid={} upAsk={} downBid={} downAsk={}",
                        slug,
                        upBid,
                        upAsk,
                        downBid,
                        downAsk
                );

                latestSnapshot.set(null);
                return;
            }

            BigDecimal strikePriceUsd =
                    referenceOpenPrice.get();

            if (strikePriceUsd == null) {

                log.info(
                        "REFRESH result=NO_REFERENCE_PRICE_YET slug={} " +
                                "price={} avg60sPrice={}",
                        slug,
                        prices.getPrice(SYMBOL),
                        prices.getAvg60sPrice(SYMBOL)
                );

                latestSnapshot.set(null);
                return;
            }

            /*
             * IMPORTANT:
             *
             * upAsk/downAsk are BEST ASKS, not 1-UP.
             *
             * The bot is a taker on entry, so these represent the
             * approximate executable prices for BUYING UP/DOWN.
             *
             * upBid/downBid are the corresponding BEST BIDS, i.e. the
             * approximate executable prices for SELLING an already-held
             * UP/DOWN position back to the market.
             */
            latestSnapshot.set(
                    new PolymarketMarketSnapshot(
                            slug,
                            upAsk,
                            downAsk,
                            upBid,
                            downBid,
                            secondsUntilClose,
                            strikePriceUsd,
                            secondsSinceOpen
                    )
            );

            logSnapshot(
                    slug,
                    upBid,
                    upAsk,
                    downBid,
                    downAsk,
                    secondsUntilClose,
                    secondsSinceOpen,
                    strikePriceUsd
            );

        } catch (Exception e) {

            log.error(
                    "REFRESH result=ERROR " +
                            "detail='exception during Polymarket market refresh'",
                    e
            );
        }
    }

    private void handleWindowChange(String slug) {

        String previousSlug =
                currentSlug.getAndSet(slug);

        if (!slug.equals(previousSlug)) {

            referenceOpenPrice.set(null);

            upBestBid.set(null);
            upBestAsk.set(null);
            downBestBid.set(null);
            downBestAsk.set(null);

            latestSnapshot.set(null);

            lastLoggedSignature.set(null);

            log.info(
                    "REFRESH result=NEW_WINDOW slug={} previousSlug={}",
                    slug,
                    previousSlug
            );
        }
    }

    private void captureReferencePriceIfNeeded(
            String slug,
            long secondsSinceOpen) {

        if (referenceOpenPrice.get() != null) {
            return;
        }

        BigDecimal openPrice =
                prices.getAvg60sPrice(SYMBOL) != null
                        ? prices.getAvg60sPrice(SYMBOL)
                        : prices.getPrice(SYMBOL);

        if (openPrice == null) {

            log.info(
                    "REFRESH result=NO_REFERENCE_PRICE_YET slug={} " +
                            "secondsSinceOpen={} price={} avg60sPrice={}",
                    slug,
                    secondsSinceOpen,
                    prices.getPrice(SYMBOL),
                    prices.getAvg60sPrice(SYMBOL)
            );

            return;
        }

        referenceOpenPrice.set(openPrice);

        log.info(
                "REFRESH result=REFERENCE_PRICE_CAPTURED " +
                        "slug={} referenceOpenPrice={} secondsSinceOpen={}",
                slug,
                openPrice,
                secondsSinceOpen
        );
    }

    /**
     * Rebuilds the snapshot immediately when a WebSocket price changes.
     *
     * This is the important part: TradingDecisionService no longer
     * has to wait for the next Gamma refresh to see a new UP/DOWN price
     * (buy side) or a new bid (sell side).
     */
    private void rebuildSnapshot(String slug) {

        if (!slug.equals(currentSlug.get())) {
            return;
        }

        BigDecimal upAsk = upBestAsk.get();
        BigDecimal downAsk = downBestAsk.get();

        BigDecimal upBid = upBestBid.get();
        BigDecimal downBid = downBestBid.get();

        BigDecimal strikePriceUsd =
                referenceOpenPrice.get();

        if (upAsk == null
                || downAsk == null
                || strikePriceUsd == null) {
            return;
        }

        try {

            /*
             * We need the market end time to calculate the
             * live remaining time.
             *
             * The existing snapshot remains valid between refreshes,
             * so don't make a Gamma request for every WS tick.
             */
            PolymarketMarketSnapshot existing =
                    latestSnapshot.get();

            if (existing == null
                    || !slug.equals(existing.slug())) {
                return;
            }

            long secondsUntilClose =
                    existing.secondsUntilClose();

            long secondsSinceOpen =
                    existing.secondsSinceOpen();

            latestSnapshot.set(
                    new PolymarketMarketSnapshot(
                            slug,
                            upAsk,
                            downAsk,
                            upBid,
                            downBid,
                            secondsUntilClose,
                            strikePriceUsd,
                            secondsSinceOpen
                    )
            );

            log.debug(
                    "CLOB UPDATE slug={} UP bid={} ask={} DOWN bid={} ask={}",
                    slug,
                    upBid,
                    upAsk,
                    downBid,
                    downAsk
            );

        } catch (Exception e) {

            log.error(
                    "Failed rebuilding Polymarket snapshot from CLOB update",
                    e
            );
        }
    }

    private void logSnapshot(
            String slug,
            BigDecimal upBid,
            BigDecimal upAsk,
            BigDecimal downBid,
            BigDecimal downAsk,
            long secondsUntilClose,
            long secondsSinceOpen,
            BigDecimal strikePriceUsd) {

        String signature =
                slug
                        + "|" + upBid
                        + "|" + upAsk
                        + "|" + downBid
                        + "|" + downAsk
                        + "|" + strikePriceUsd;

        String previousSignature =
                lastLoggedSignature.getAndSet(signature);

        boolean changed =
                !signature.equals(previousSignature);

        boolean heartbeatDue =
                Duration.between(
                                lastSnapshotHeartbeatAt.get(),
                                Instant.now()
                        )
                        .compareTo(
                                SNAPSHOT_HEARTBEAT_INTERVAL
                        ) >= 0;

        if (changed || heartbeatDue) {

            if (heartbeatDue) {
                lastSnapshotHeartbeatAt.set(
                        Instant.now()
                );
            }

            log.info(
                    "REFRESH result=SNAPSHOT_OK slug={} " +
                            "UP[bid={},ask={}] " +
                            "DOWN[bid={},ask={}] " +
                            "secondsUntilClose={} " +
                            "secondsSinceOpen={} " +
                            "strikePriceUsd={} changed={}",
                    slug,
                    upBid,
                    upAsk,
                    downBid,
                    downAsk,
                    secondsUntilClose,
                    secondsSinceOpen,
                    strikePriceUsd,
                    changed
            );
        }
    }

    private long parseEndDate(
            JsonNode market,
            long fallback) {

        JsonNode endDateNode =
                market.get("endDate");

        if (endDateNode == null
                || endDateNode.isNull()
                || endDateNode.asText().isBlank()) {

            return fallback;
        }

        try {

            return Instant
                    .parse(endDateNode.asText())
                    .toEpochMilli();

        } catch (Exception e) {

            log.warn(
                    "Could not parse Polymarket endDate: {}",
                    endDateNode
            );

            return fallback;
        }
    }

    private String computeCurrentSlug() {

        long nowSeconds =
                System.currentTimeMillis() / 1000;

        long windowSeconds =
                WINDOW.toSeconds();

        long windowStart =
                (nowSeconds / windowSeconds)
                        * windowSeconds;

        return SLUG_PREFIX + windowStart;
    }

    private JsonNode fetchEventBySlug(
            String slug) {

        ResponseEntity<JsonNode> response =
                gammaWebClient.get()
                        .uri(uriBuilder ->
                                uriBuilder
                                        .path("/events")
                                        .queryParam("slug", slug)
                                        .build()
                        )
                        .retrieve()
                        .toEntity(JsonNode.class)
                        .onErrorResume(e -> {

                            log.warn(
                                    "Gamma event lookup failed for slug {}: {}",
                                    slug,
                                    e.toString()
                            );

                            return Mono.empty();
                        })
                        .block();

        if (response == null) {
            return null;
        }

        recordGammaClockSample(
                response
                        .getHeaders()
                        .getFirst(HttpHeaders.DATE)
        );

        JsonNode responseBody =
                response.getBody();

        if (responseBody == null) {
            return null;
        }

        if (responseBody.isArray()) {

            return responseBody.isEmpty()
                    ? null
                    : responseBody.get(0);
        }

        return responseBody;
    }

    private void recordGammaClockSample(
            String httpDateHeader) {

        if (httpDateHeader == null) {
            return;
        }

        try {

            Instant serverInstant =
                    Instant.from(
                            DateTimeFormatter.RFC_1123_DATE_TIME
                                    .parse(httpDateHeader)
                    );

            polymarketClock.recordServerTimestamp(
                    serverInstant.toEpochMilli()
            );

        } catch (Exception e) {

            log.debug(
                    "Failed to parse Gamma Date header '{}'",
                    httpDateHeader
            );
        }
    }
}