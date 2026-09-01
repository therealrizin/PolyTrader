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
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves the currently-open Polymarket "BTC Up or Down" 5-minute market
 * via the public Gamma API and exposes it as a {@link PolymarketMarketSnapshot}.
 *
 * The window-open reference price is now taken directly from Chainlink's
 * price (via {@link Prices}, fed by {@link ChainlinkPriceStreamClient}) —
 * the 60s TWAP is preferred, falling back to the raw price, with no
 * exchange blending involved anymore.
 */
@Slf4j
@Component
public class PolymarketDataProvider {

    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final String SLUG_PREFIX = "btc-updown-5m-";
    private static final Duration SNAPSHOT_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;

    private final WebClient gammaWebClient;
    private final ObjectMapper objectMapper;
    private final PolymarketProperties properties;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PolymarketClock polymarketClock;

    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicReference<BigDecimal> referenceOpenPrice = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot = new AtomicReference<>();

    private final AtomicReference<String> lastLoggedSignature = new AtomicReference<>();
    private final AtomicReference<Instant> lastSnapshotHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    public PolymarketDataProvider(@Qualifier("gammaWebClient") WebClient gammaWebClient,
                                  ObjectMapper objectMapper,
                                  PolymarketProperties properties,
                                  TaskScheduler liveDataTaskScheduler,
                                  Prices prices,
                                  PolymarketClock polymarketClock) {
        this.gammaWebClient = gammaWebClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.polymarketClock = polymarketClock;
    }

    @PostConstruct
    public void start() {
        if (properties.gammaBaseUrl() == null || properties.gammaBaseUrl().isBlank()) {
            throw new IllegalStateException(
                    "services.polymarket.gamma-base-url is not set. Gamma API calls cannot succeed without it " +
                            "(expected something like https://gamma-api.polymarket.com). Check application.yaml.");
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

    private void refresh() {
        try {
            String slug = computeCurrentSlug();

            JsonNode event = fetchEventBySlug(slug);
            if (event == null) {
                log.info("REFRESH result=NO_EVENT slug={} detail='Gamma returned no event for this slug'", slug);
                latestSnapshot.set(null);
                return;
            }

            JsonNode marketsNode = event.get("markets");
            if (marketsNode == null || !marketsNode.isArray() || marketsNode.isEmpty()) {
                log.warn("REFRESH result=NO_MARKETS slug={} detail='event found but markets[] missing/empty'", slug);
                latestSnapshot.set(null);
                return;
            }
            JsonNode market = marketsNode.get(0);

            List<BigDecimal> priceResult = resolveMarketPrices(market, slug);
            if (priceResult == null) {
                latestSnapshot.set(null);
                return;
            }
            BigDecimal upPrice = priceResult.get(0);
            BigDecimal downPrice = priceResult.get(1);

            long nowMillis = polymarketClock.nowMillis();

            JsonNode endDateNode = market.get("endDate");
            long endEpochMillis = endDateNode != null
                    ? Instant.parse(endDateNode.stringValue()).toEpochMilli()
                    : nowMillis;
            long secondsUntilClose = Math.max(0, (endEpochMillis - nowMillis) / 1000);

            long windowOpenEpochMillis = endEpochMillis - WINDOW.toMillis();
            long secondsSinceOpen = Math.max(0, (nowMillis - windowOpenEpochMillis) / 1000);

            String previousSlug = currentSlug.getAndSet(slug);
            if (!slug.equals(previousSlug)) {
                referenceOpenPrice.set(null);
                log.info("REFRESH result=NEW_WINDOW slug={} secondsUntilClose={}", slug, secondsUntilClose);
            }

            BigDecimal strikePriceUsd = referenceOpenPrice.get();
            if (strikePriceUsd == null) {
                BigDecimal openPrice = prices.getAvg60sPrice(SYMBOL) != null
                        ? prices.getAvg60sPrice(SYMBOL)
                        : prices.getPrice(SYMBOL);
                if (openPrice != null) {
                    referenceOpenPrice.set(openPrice);
                    strikePriceUsd = openPrice;
                    log.info("REFRESH result=REFERENCE_PRICE_CAPTURED slug={} referenceOpenPrice={}", slug, openPrice);
                } else {
                    log.info("REFRESH result=NO_REFERENCE_PRICE_YET slug={} price={} avg60sPrice={} " +
                                    "detail='no Chainlink tick received yet — check ChainlinkPriceStreamClient connection logs'",
                            slug, prices.getPrice(SYMBOL), prices.getAvg60sPrice(SYMBOL));
                    latestSnapshot.set(null);
                    return;
                }
            }

            latestSnapshot.set(new PolymarketMarketSnapshot(
                    slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd));
            logSnapshot(slug, upPrice, downPrice, secondsUntilClose, secondsSinceOpen, strikePriceUsd);
        } catch (Exception e) {
            log.error("REFRESH result=ERROR detail='exception during Polymarket market refresh'", e);
            latestSnapshot.set(null);
        }
    }

    private void logSnapshot(String slug, BigDecimal upPrice, BigDecimal downPrice,
                             long secondsUntilClose, long secondsSinceOpen, BigDecimal strikePriceUsd) {
        String signature = slug + "|" + upPrice + "|" + downPrice + "|" + strikePriceUsd;
        String previousSignature = lastLoggedSignature.getAndSet(signature);
        boolean changed = !signature.equals(previousSignature);

        boolean heartbeatDue = Duration.between(lastSnapshotHeartbeatAt.get(), Instant.now())
                .compareTo(SNAPSHOT_HEARTBEAT_INTERVAL) >= 0;

        if (changed || heartbeatDue) {
            if (heartbeatDue) lastSnapshotHeartbeatAt.set(Instant.now());
            log.info("REFRESH result=SNAPSHOT_OK slug={} upPrice={} downPrice={} secondsUntilClose={} " +
                            "secondsSinceOpen={} strikePriceUsd={} changed={}",
                    slug, upPrice, downPrice, secondsUntilClose, secondsSinceOpen, strikePriceUsd, changed);
        } else {
            log.debug("REFRESH result=SNAPSHOT_OK slug={} upPrice={} downPrice={} secondsUntilClose={} " +
                            "secondsSinceOpen={} strikePriceUsd={} changed=false",
                    slug, upPrice, downPrice, secondsUntilClose, secondsSinceOpen, strikePriceUsd);
        }
    }

    private List<BigDecimal> resolveMarketPrices(JsonNode market, String slug) {
        BigDecimal bestBid = parseDecimalField(market.get("bestBid"));
        BigDecimal bestAsk = parseDecimalField(market.get("bestAsk"));
        BigDecimal lastTradePrice = parseDecimalField(market.get("lastTradePrice"));

        BigDecimal upPrice;
        String source;

        if (bestBid != null && bestAsk != null
                && bestBid.signum() > 0 && bestAsk.signum() > 0
                && bestBid.compareTo(BigDecimal.ONE) < 0 && bestAsk.compareTo(BigDecimal.ONE) < 0) {
            upPrice = bestBid.add(bestAsk).divide(BigDecimal.valueOf(2), 6, RoundingMode.HALF_UP);
            source = "BEST_BID_ASK_MID";
        } else if (lastTradePrice != null && lastTradePrice.signum() > 0 && lastTradePrice.compareTo(BigDecimal.ONE) < 0) {
            upPrice = lastTradePrice;
            source = "LAST_TRADE_PRICE";
        } else {
            List<BigDecimal> outcomePrices = parseJsonDecimalArray(market.get("outcomePrices"));
            if (outcomePrices.size() < 2) {
                log.warn("REFRESH result=BAD_OUTCOME_PRICES slug={} bestBid={} bestAsk={} lastTradePrice={} " +
                                "outcomePricesRaw={} outcomePricesParsed={}",
                        slug, bestBid, bestAsk, lastTradePrice, market.get("outcomePrices"), outcomePrices);
                return null;
            }
            upPrice = outcomePrices.get(0);
            source = "OUTCOME_PRICES_ARRAY";
        }

        BigDecimal downPrice = BigDecimal.ONE.subtract(upPrice);

        log.debug("REFRESH priceSource={} slug={} bestBid={} bestAsk={} lastTradePrice={} resolvedUpPrice={}",
                source, slug, bestBid, bestAsk, lastTradePrice, upPrice);

        return List.of(upPrice, downPrice);
    }

    private BigDecimal parseDecimalField(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            return node.isNumber() ? node.decimalValue() : new BigDecimal(node.stringValue());
        } catch (Exception e) {
            log.debug("Failed to parse numeric field '{}'", node, e);
            return null;
        }
    }

    private String computeCurrentSlug() {
        long nowSeconds = System.currentTimeMillis() / 1000;
        long windowSeconds = WINDOW.toSeconds();
        long windowStart = (nowSeconds / windowSeconds) * windowSeconds;
        return SLUG_PREFIX + windowStart;
    }

    private JsonNode fetchEventBySlug(String slug) {
        ResponseEntity<JsonNode> response = gammaWebClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/events")
                        .queryParam("slug", slug)
                        .build())
                .retrieve()
                .toEntity(JsonNode.class)
                .onErrorResume(e -> {
                    log.warn("Gamma event lookup failed for slug {}: {}", slug, e.toString());
                    return Mono.empty();
                })
                .block();

        if (response == null) return null;

        recordGammaClockSample(response.getHeaders().getFirst(HttpHeaders.DATE));

        JsonNode responseBody = response.getBody();
        if (responseBody == null) return null;

        if (responseBody.isArray()) {
            return responseBody.isEmpty() ? null : responseBody.get(0);
        }

        return responseBody;
    }

    private void recordGammaClockSample(String httpDateHeader) {
        if (httpDateHeader == null) return;
        try {
            Instant serverInstant = Instant.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(httpDateHeader));
            polymarketClock.recordServerTimestamp(serverInstant.toEpochMilli());
        } catch (Exception e) {
            log.debug("Failed to parse Gamma 'Date' response header '{}'", httpDateHeader, e);
        }
    }

    private List<BigDecimal> parseJsonDecimalArray(JsonNode node) {
        if (node == null) return List.of();

        try {
            JsonNode arr = node.isArray() ? node : objectMapper.readTree(node.stringValue());
            List<BigDecimal> result = new ArrayList<>();
            arr.forEach(n -> result.add(new BigDecimal(n.stringValue())));
            return result;
        } catch (Exception e) {
            log.warn("Failed to parse Gamma JSON-array field '{}'", node, e);
            return List.of();
        }
    }
}