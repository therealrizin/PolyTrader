package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class PolymarketDataProvider {

    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final String SLUG_PREFIX = "btc-updown-5m-";
    private static final Duration SNAPSHOT_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    private final WebClient gammaWebClient;
    private final ObjectMapper objectMapper;
    private final PolymarketProperties properties;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PolymarketMarketWebSocketClient webSocketClient;

    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicReference<BigDecimal> referenceOpenPrice = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot = new AtomicReference<>();

    // WebSocket‑fed real‑time prices for each side (midpoint)
    private final ConcurrentHashMap<MarketSide, BigDecimal> livePrices = new ConcurrentHashMap<>();

    // De-duplication state for logging.
    private final AtomicReference<String> lastLoggedSignature = new AtomicReference<>();
    private final AtomicReference<Instant> lastSnapshotHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    public PolymarketDataProvider(@Qualifier("gammaWebClient") WebClient gammaWebClient,
                                  ObjectMapper objectMapper,
                                  PolymarketProperties properties,
                                  TaskScheduler liveDataTaskScheduler,
                                  Prices prices,
                                  PolymarketMarketWebSocketClient webSocketClient) {
        this.gammaWebClient = gammaWebClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.webSocketClient = webSocketClient;
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

            // -------- Detect new window and subscribe to WebSocket --------
            String previousSlug = currentSlug.getAndSet(slug);
            if (!slug.equals(previousSlug)) {
                referenceOpenPrice.set(null);
                livePrices.clear();
                log.info("REFRESH result=NEW_WINDOW slug={}", slug);
                // Subscribe to the new market via WebSocket
                webSocketClient.subscribe(slug, this::onWebSocketPriceUpdate);
            }

            // -------- Capture reference open price (if not set) --------
            if (referenceOpenPrice.get() == null) {
                BigDecimal openPrice = prices.getAvg60sPrice() != null ? prices.getAvg60sPrice() : prices.getAvgPrice();
                if (openPrice != null) {
                    referenceOpenPrice.set(openPrice);
                    log.info("REFRESH result=REFERENCE_PRICE_CAPTURED slug={} referenceOpenPrice={}", slug, openPrice);
                } else {
                    log.debug("REFRESH result=NO_REFERENCE_PRICE_YET slug={} avgPrice={} avg60sPrice={}",
                            slug, prices.getAvgPrice(), prices.getAvg60sPrice());
                    // Still try to produce a snapshot if we have WebSocket prices; otherwise keep null.
                    updateSnapshotFromWebSocketOrGamma(slug);
                    return;
                }
            }

            // -------- Build snapshot from WebSocket (preferred) or Gamma (fallback) --------
            updateSnapshotFromWebSocketOrGamma(slug);

        } catch (Exception e) {
            log.error("REFRESH result=ERROR detail='exception during Polymarket market refresh'", e);
            latestSnapshot.set(null);
        }
    }

    /**
     * Updates the snapshot using live WebSocket prices if available;
     * otherwise falls back to the Gamma REST API.
     */
    private void updateSnapshotFromWebSocketOrGamma(String slug) {
        BigDecimal upPrice = livePrices.get(MarketSide.UP);
        BigDecimal downPrice = livePrices.get(MarketSide.DOWN);

        if (upPrice != null && downPrice != null) {
            // We have live WebSocket prices – use them
            long endEpochMillis = fetchEndEpochFromGamma(slug);
            long secondsUntilClose = Math.max(0, (endEpochMillis - System.currentTimeMillis()) / 1000);
            BigDecimal strike = referenceOpenPrice.get();
            if (strike == null) {
                log.debug("No reference price yet, cannot build snapshot");
                return;
            }
            latestSnapshot.set(new PolymarketMarketSnapshot(slug, upPrice, downPrice, secondsUntilClose, strike));
            logSnapshot(slug, upPrice, downPrice, secondsUntilClose, strike);
        } else {
            // Fallback to Gamma REST snapshot
            BigDecimal[] pricesFromGamma = fetchGammaPrices(slug);
            if (pricesFromGamma == null) {
                latestSnapshot.set(null);
                return;
            }
            long endEpochMillis = fetchEndEpochFromGamma(slug);
            long secondsUntilClose = Math.max(0, (endEpochMillis - System.currentTimeMillis()) / 1000);
            BigDecimal strike = referenceOpenPrice.get();
            if (strike == null) {
                log.debug("No reference price yet, cannot build snapshot");
                return;
            }
            latestSnapshot.set(new PolymarketMarketSnapshot(slug, pricesFromGamma[0], pricesFromGamma[1],
                    secondsUntilClose, strike));
            logSnapshot(slug, pricesFromGamma[0], pricesFromGamma[1], secondsUntilClose, strike);
        }
    }

    /**
     * Callback from the WebSocket client – updates the livePrices map.
     */
    private void onWebSocketPriceUpdate(MarketSide side, BigDecimal bestBid, BigDecimal bestAsk) {
        // Compute midpoint as the market price for this side
        if (bestBid != null && bestAsk != null) {
            BigDecimal mid = bestBid.add(bestAsk).divide(BigDecimal.valueOf(2), 6, RoundingMode.HALF_UP);
            livePrices.put(side, mid);
            log.debug("WebSocket price update: side={} mid={}", side, mid);
        } else {
            // If one side is missing, we cannot compute a reliable midpoint; remove the stale value.
            livePrices.remove(side);
            log.debug("WebSocket price update: side={} has incomplete BBO (bid={}, ask={}) – removed", side, bestBid, bestAsk);
        }
    }

    // ------------------------------------------------------------------------
    // Gamma REST helpers
    // ------------------------------------------------------------------------
    private BigDecimal[] fetchGammaPrices(String slug) {
        JsonNode market = fetchMarketBySlug(slug);
        if (market == null) return null;
        List<BigDecimal> priceResult = resolveMarketPrices(market, slug);
        if (priceResult == null) return null;
        return new BigDecimal[]{priceResult.get(0), priceResult.get(1)};
    }

    private long fetchEndEpochFromGamma(String slug) {
        JsonNode market = fetchMarketBySlug(slug);
        if (market == null) return System.currentTimeMillis() + 5 * 60 * 1000; // fallback 5 min
        JsonNode endDateNode = market.get("endDate");
        if (endDateNode != null) {
            try {
                return Instant.parse(endDateNode.stringValue()).toEpochMilli();
            } catch (Exception e) {
                log.warn("Failed to parse endDate: {}", endDateNode, e);
            }
        }
        return System.currentTimeMillis() + 5 * 60 * 1000;
    }

    private JsonNode fetchMarketBySlug(String slug) {
        JsonNode event = fetchEventBySlug(slug);
        if (event == null) return null;
        JsonNode marketsNode = event.get("markets");
        if (marketsNode == null || !marketsNode.isArray() || marketsNode.isEmpty()) {
            log.warn("REFRESH result=NO_MARKETS slug={}", slug);
            return null;
        }
        return marketsNode.get(0);
    }

    /**
     * Slug timestamp is the window's OPEN time (floor to the nearest 300s
     * boundary), not the close time — see class javadoc.
     */
    private String computeCurrentSlug() {
        long nowSeconds = System.currentTimeMillis() / 1000;
        long windowSeconds = WINDOW.toSeconds();
        long windowStart = (nowSeconds / windowSeconds) * windowSeconds;
        return SLUG_PREFIX + windowStart;
    }

    /**
     * GET /events?slug={slug} — query parameter, NOT a path segment.
     * Gamma returns a JSON ARRAY of matching events (typically 0 or 1
     * elements for an exact slug match), not a single event object.
     */
    private JsonNode fetchEventBySlug(String slug) {
        JsonNode response = gammaWebClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/events")
                        .queryParam("slug", slug)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> {
                    log.warn("Gamma event lookup failed for slug {}: {}", slug, e.toString());
                    return Mono.empty();
                })
                .block();

        if (response == null) return null;

        if (response.isArray()) {
            return response.isEmpty() ? null : response.get(0);
        }

        // Defensive: if Gamma ever returns a single object instead of an
        // array (e.g. API change), still handle it rather than silently
        // returning nothing.
        return response;
    }

    /**
     * Resolves the Up/Down market prices with a priority order that
     * prefers genuinely live signals over the stale last-trade price:
     *
     *   1. bestBid/bestAsk midpoint — tracks the live order book,
     *      updates continuously even without a trade printing.
     *   2. lastTradePrice — updates only when a trade executes; can be
     *      stale for a long time on a thin/short-dated market.
     *   3. outcomePrices array — same staleness caveat as
     *      lastTradePrice, kept only as a last-resort fallback in case a
     *      payload omits the other fields.
     *
     * ASSUMPTION: the single top-level bestBid/bestAsk/lastTradePrice fields,
     * and index 0 of outcomePrices, all refer to the "Up" outcome; "Down" is its
     * complement (1 - price).
     *
     * Returns null (and logs the reason) if no usable price could be resolved.
     */
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

    /**
     * Handles both encodings Gamma has been observed to use for this
     * field: a JSON-array-encoded STRING (e.g. "[\"0.5\",\"0.5\"]"), or a
     * native JSON array node.
     */
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

    // ------------------------------------------------------------------------
    // Logging
    // ------------------------------------------------------------------------
    private void logSnapshot(String slug, BigDecimal upPrice, BigDecimal downPrice,
                             long secondsUntilClose, BigDecimal strikePriceUsd) {
        String signature = slug + "|" + upPrice + "|" + downPrice + "|" + strikePriceUsd;
        String previousSignature = lastLoggedSignature.getAndSet(signature);
        boolean changed = !signature.equals(previousSignature);

        boolean heartbeatDue = Duration.between(lastSnapshotHeartbeatAt.get(), Instant.now())
                .compareTo(SNAPSHOT_HEARTBEAT_INTERVAL) >= 0;

        if (changed || heartbeatDue) {
            if (heartbeatDue) lastSnapshotHeartbeatAt.set(Instant.now());
            log.info("REFRESH result=SNAPSHOT_OK slug={} upPrice={} downPrice={} secondsUntilClose={} " +
                            "strikePriceUsd={} changed={}",
                    slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd, changed);
        } else {
            log.debug("REFRESH result=SNAPSHOT_OK slug={} upPrice={} downPrice={} secondsUntilClose={} " +
                            "strikePriceUsd={} changed=false",
                    slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd);
        }
    }
}