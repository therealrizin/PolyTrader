package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.model.MarketSide;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class PolymarketDataProvider {

    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final String BTC_SLUG_PREFIX = "btc-updown-5m-";
    private static final String CRYPTO_PRICE_PATH = "/api/crypto/crypto-price";
    private static final String BTC_CRYPTO_SYMBOL = "BTC";
    private static final String CRYPTO_VARIANT = "fiveminute";
    private static final boolean TWAP_ENABLED = true;
    private static final int TWAP_LOOKBACK_SECONDS = 60;

    private final WebClient gammaWebClient;
    private final WebClient polymarketWebClient;
    private final PolymarketProperties properties;
    private final TaskScheduler liveDataTaskScheduler;
    private final PolymarketClock polymarketClock;
    private final PolymarketMarketWebSocketClient marketWebSocketClient;

    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicReference<Long> currentEndEpochMillis = new AtomicReference<>();
    private final AtomicReference<Long> currentWindowOpenEpochMillis = new AtomicReference<>();
    private final AtomicReference<BigDecimal> referenceOpenPrice = new AtomicReference<>();
    private final AtomicReference<BigDecimal> upBestBid = new AtomicReference<>();
    private final AtomicReference<BigDecimal> upBestAsk = new AtomicReference<>();
    private final AtomicReference<BigDecimal> downBestBid = new AtomicReference<>();
    private final AtomicReference<BigDecimal> downBestAsk = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot = new AtomicReference<>();
    private final AtomicReference<String> lastLoggedSignature = new AtomicReference<>();
    private final AtomicReference<Instant> lastSnapshotHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    public PolymarketDataProvider(
            @Qualifier("gammaWebClient")
            WebClient gammaWebClient,
            @Qualifier("polymarketWebClient")
            WebClient polymarketWebClient,
            PolymarketProperties properties,
            TaskScheduler liveDataTaskScheduler,
            Prices prices,
            PolymarketClock polymarketClock,
            PolymarketMarketWebSocketClient marketWebSocketClient) {

        this.gammaWebClient = gammaWebClient;
        this.polymarketWebClient = polymarketWebClient;
        this.properties = properties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.polymarketClock = polymarketClock;
        this.marketWebSocketClient = marketWebSocketClient;
    }

    @PostConstruct
    public void start() {
        if (properties.gammaBaseUrl() == null || properties.gammaBaseUrl().isBlank()) {
            throw new IllegalStateException("services.polymarket.gamma-base-url is not configured");
        }
        log.info("PolymarketDataProvider starting: gammaBaseUrl={} marketRefreshSeconds={}",
                properties.gammaBaseUrl(), properties.marketRefreshSeconds());
        refresh();
        int refreshSeconds = properties.marketRefreshSeconds() > 0 ? properties.marketRefreshSeconds() : 5;
        liveDataTaskScheduler.scheduleAtFixedRate(this::refresh, Duration.ofSeconds(refreshSeconds));
    }

    public Optional<PolymarketMarketSnapshot> currentSnapshot() {
        return Optional.ofNullable(latestSnapshot.get());
    }

    private void onPriceUpdate(String callbackSlug, MarketSide side, BigDecimal bestBid, BigDecimal bestAsk) {
        String slug = currentSlug.get();

        if (slug == null || !slug.equals(callbackSlug)) {
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

        rebuildSnapshot(
                callbackSlug
        );
    }

    private void refresh() {
        try {
            String slug = computeCurrentSlug();
            JsonNode event = fetchEventBySlug(slug);
            if (event == null) {
                log.info("REFRESH result=NO_EVENT slug={}", slug);
                if (slug.equals(currentSlug.get())) {
                    latestSnapshot.set(null);
                }
                return;
            }

            JsonNode marketsNode = event.get("markets");

            if (marketsNode == null || !marketsNode.isArray() || marketsNode.isEmpty()) {
                log.warn("REFRESH result=NO_MARKETS slug={}", slug);
                latestSnapshot.set(null);
                return;
            }

            JsonNode market = marketsNode.get(0);
            long nowMillis = polymarketClock.nowMillis();
            Long endEpochMillis = parseEndDate(market);

            if (endEpochMillis == null) {
                log.warn("REFRESH result=NO_VALID_END_DATE slug={}", slug);
                latestSnapshot.set(null);
                return;
            }

            long windowOpenEpochMillis = endEpochMillis - WINDOW.toMillis();
            long secondsUntilClose = secondsUntil(endEpochMillis, nowMillis);
            long secondsSinceOpen = secondsSince(windowOpenEpochMillis, nowMillis);

            handleWindowChange(slug, windowOpenEpochMillis, endEpochMillis);

            BigDecimal strikePriceUsd = referenceOpenPrice.get();

            if (strikePriceUsd == null) {
                strikePriceUsd = fetchPriceToBeat(windowOpenEpochMillis, endEpochMillis);

                if (strikePriceUsd == null) {
                    log.warn("REFRESH result=NO_OFFICIAL_PRICE_TO_BEAT slug={} secondsSinceOpen={}",
                            slug, secondsSinceOpen);
                    latestSnapshot.set(null);
                    return;
                }

                if (!slug.equals(currentSlug.get())) {
                    return;
                }

                referenceOpenPrice.set(strikePriceUsd);

                log.info("REFRESH result=OFFICIAL_PRICE_TO_BEAT_CACHED slug={} priceToBeat={} secondsSinceOpen={}",
                        slug, strikePriceUsd, secondsSinceOpen);

            } else {
                log.debug("REFRESH result=USING_CACHED_PRICE_TO_BEAT slug={} priceToBeat={}",
                        slug, strikePriceUsd);
            }

            if (!slug.equals(currentSlug.get())) {
                return;
            }

            currentEndEpochMillis.set(endEpochMillis);
            currentWindowOpenEpochMillis.set(windowOpenEpochMillis);
            marketWebSocketClient.subscribe(slug,
                    (side, bid, ask) ->
                            onPriceUpdate(slug, side, bid, ask));

            rebuildSnapshot(slug, secondsUntilClose, secondsSinceOpen);

        } catch (Exception e) {
            log.error("REFRESH result=ERROR", e);
        }
    }

    private void rebuildSnapshot(
            String slug) {

        Long endMillis =
                currentEndEpochMillis.get();

        Long openMillis =
                currentWindowOpenEpochMillis.get();

        if (endMillis == null
                || openMillis == null) {

            return;
        }

        long nowMillis =
                polymarketClock.nowMillis();

        rebuildSnapshot(
                slug,
                secondsUntil(
                        endMillis,
                        nowMillis
                ),
                secondsSince(
                        openMillis,
                        nowMillis
                )
        );
    }

    private void rebuildSnapshot(
            String slug,
            long secondsUntilClose,
            long secondsSinceOpen) {

        if (!slug.equals(
                currentSlug.get()
        )) {

            return;
        }

        BigDecimal strikePriceUsd =
                referenceOpenPrice.get();

        BigDecimal upAsk =
                upBestAsk.get();

        BigDecimal downAsk =
                downBestAsk.get();

        BigDecimal upBid =
                upBestBid.get();

        BigDecimal downBid =
                downBestBid.get();

        if (strikePriceUsd == null
                || upAsk == null
                || downAsk == null) {

            return;
        }

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
                "SNAPSHOT UPDATED slug={} "
                        + "UP[bid={},ask={}] "
                        + "DOWN[bid={},ask={}] "
                        + "secondsLeft={} "
                        + "priceToBeat={}",
                slug,
                upBid,
                upAsk,
                downBid,
                downAsk,
                secondsUntilClose,
                strikePriceUsd
        );
    }

    private void handleWindowChange(
            String slug,
            long windowOpenEpochMillis,
            long endEpochMillis) {

        String previousSlug =
                currentSlug.getAndSet(slug);

        boolean changed =
                !slug.equals(previousSlug);

        if (!changed) {
            return;
        }

        /*
         * New 5-minute market.
         *
         * Completely discard previous market state.
         */
        referenceOpenPrice.set(null);

        upBestBid.set(null);
        upBestAsk.set(null);

        downBestBid.set(null);
        downBestAsk.set(null);

        latestSnapshot.set(null);

        currentWindowOpenEpochMillis.set(
                windowOpenEpochMillis
        );

        currentEndEpochMillis.set(
                endEpochMillis
        );

        lastLoggedSignature.set(null);

        lastSnapshotHeartbeatAt.set(
                Instant.EPOCH
        );

        log.info(
                "REFRESH result=NEW_WINDOW "
                        + "slug={} previousSlug={}",
                slug,
                previousSlug
        );
    }

    private BigDecimal fetchPriceToBeat(
            long eventStartEpochMillis,
            long endEpochMillis) {

        String eventStartTime =
                Instant
                        .ofEpochMilli(
                                eventStartEpochMillis
                        )
                        .toString();

        String endDate =
                Instant
                        .ofEpochMilli(
                                endEpochMillis
                        )
                        .toString();

        try {

            ResponseEntity<JsonNode> response =
                    polymarketWebClient
                            .get()
                            .uri(uriBuilder ->
                                    uriBuilder
                                            .path(
                                                    CRYPTO_PRICE_PATH
                                            )
                                            .queryParam(
                                                    "symbol",
                                                    BTC_CRYPTO_SYMBOL
                                            )
                                            .queryParam(
                                                    "eventStartTime",
                                                    eventStartTime
                                            )
                                            .queryParam(
                                                    "variant",
                                                    CRYPTO_VARIANT
                                            )
                                            .queryParam(
                                                    "endDate",
                                                    endDate
                                            )
                                            .queryParam(
                                                    "twapEnabled",
                                                    TWAP_ENABLED
                                            )
                                            .queryParam(
                                                    "twapLookbackSeconds",
                                                    TWAP_LOOKBACK_SECONDS
                                            )
                                            .build()
                            )
                            .header(
                                    HttpHeaders.USER_AGENT,
                                    "Mozilla/5.0"
                            )
                            .header(
                                    HttpHeaders.ACCEPT,
                                    "application/json"
                            )
                            .retrieve()
                            .toEntity(
                                    JsonNode.class
                            )
                            .onErrorResume(
                                    e -> {

                                        log.warn(
                                                "PRICE_TO_BEAT HTTP_FAILED "
                                                        + "eventStart={} "
                                                        + "endDate={} "
                                                        + "error={}",
                                                eventStartTime,
                                                endDate,
                                                e.getMessage()
                                        );

                                        return Mono.empty();
                                    }
                            )
                            .block(
                                    Duration.ofSeconds(10)
                            );

            if (response == null) {
                return null;
            }

            JsonNode body =
                    response.getBody();

            if (body == null
                    || body.isNull()) {

                return null;
            }

            JsonNode openPriceNode =
                    body.get("openPrice");

            if (openPriceNode == null
                    || openPriceNode.isNull()) {

                log.warn(
                        "PRICE_TO_BEAT result=NO_OPEN_PRICE "
                                + "eventStart={} endDate={} response={}",
                        eventStartTime,
                        endDate,
                        body
                );

                return null;
            }

            BigDecimal openPrice =
                    parsePrice(
                            openPriceNode
                    );

            if (openPrice == null
                    || openPrice.signum() <= 0) {

                log.warn(
                        "PRICE_TO_BEAT result=INVALID_OPEN_PRICE "
                                + "openPrice={}",
                        openPriceNode
                );

                return null;
            }

            log.info(
                    "PRICE_TO_BEAT result=SUCCESS "
                            + "priceToBeat={} "
                            + "source=POLYMARKET_CRYPTO_PRICE_API "
                            + "eventStartTime={} "
                            + "endDate={} "
                            + "completed={} "
                            + "incomplete={} "
                            + "cached={}",
                    openPrice,
                    eventStartTime,
                    endDate,
                    getBooleanField(
                            body,
                            "completed"
                    ),
                    getBooleanField(
                            body,
                            "incomplete"
                    ),
                    getBooleanField(
                            body,
                            "cached"
                    )
            );

            return openPrice;

        } catch (Exception e) {

            log.error(
                    "PRICE_TO_BEAT result=ERROR "
                            + "eventStart={} endDate={} "
                            + "error={}",
                    eventStartTime,
                    endDate,
                    e.getMessage(),
                    e
            );

            return null;
        }
    }

    private BigDecimal parsePrice(
            JsonNode node) {

        try {

            if (node.isNumber()) {
                return node.decimalValue();
            }

            String value =
                    node.asText();

            if (value == null
                    || value.isBlank()) {

                return null;
            }

            return new BigDecimal(
                    value.trim()
            );

        } catch (Exception e) {

            return null;
        }
    }

    private boolean getBooleanField(
            JsonNode node,
            String field) {

        JsonNode value =
                node.get(field);

        return value != null
                && !value.isNull()
                && value.asBoolean(false);
    }

    private Long parseEndDate(
            JsonNode market) {

        JsonNode endDateNode =
                market.get("endDate");

        if (endDateNode == null
                || endDateNode.isNull()
                || endDateNode.asText().isBlank()) {

            return null;
        }

        try {

            return Instant
                    .parse(
                            endDateNode.asText()
                    )
                    .toEpochMilli();

        } catch (Exception e) {

            log.warn(
                    "Could not parse Polymarket endDate={}",
                    endDateNode
            );

            return null;
        }
    }

    private String computeCurrentSlug() {

        long nowSeconds =
                polymarketClock.nowMillis()
                        / 1000;

        long windowSeconds =
                WINDOW.toSeconds();

        long windowStart =
                (nowSeconds / windowSeconds)
                        * windowSeconds;

        return BTC_SLUG_PREFIX
                + windowStart;
    }

    private long secondsUntil(
            long endEpochMillis,
            long nowMillis) {

        return Math.max(
                0,
                (endEpochMillis - nowMillis) / 1000
        );
    }

    private long secondsSince(
            long startEpochMillis,
            long nowMillis) {

        return Math.max(
                0,
                (nowMillis - startEpochMillis) / 1000
        );
    }

    private JsonNode fetchEventBySlug(
            String slug) {

        ResponseEntity<JsonNode> response =
                gammaWebClient
                        .get()
                        .uri(
                                uriBuilder ->
                                        uriBuilder
                                                .path(
                                                        "/events"
                                                )
                                                .queryParam(
                                                        "slug",
                                                        slug
                                                )
                                                .build()
                        )
                        .retrieve()
                        .toEntity(
                                JsonNode.class
                        )
                        .onErrorResume(
                                e -> {

                                    log.warn(
                                            "Gamma event lookup failed "
                                                    + "slug={} error={}",
                                            slug,
                                            e.getMessage()
                                    );

                                    return Mono.empty();
                                }
                        )
                        .block();

        if (response == null) {
            return null;
        }

        recordGammaClockSample(
                response
                        .getHeaders()
                        .getFirst(
                                HttpHeaders.DATE
                        )
        );

        JsonNode body =
                response.getBody();

        if (body == null) {
            return null;
        }

        if (body.isArray()) {

            return body.isEmpty()
                    ? null
                    : body.get(0);
        }

        return body;
    }

    private void recordGammaClockSample(
            String httpDateHeader) {

        if (httpDateHeader == null
                || httpDateHeader.isBlank()) {

            return;
        }

        try {

            Instant serverInstant =
                    Instant.from(
                            DateTimeFormatter
                                    .RFC_1123_DATE_TIME
                                    .parse(
                                            httpDateHeader
                                    )
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
