package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves the currently-open Polymarket "BTC Up or Down" 5-minute market
 * (e.g. https://polymarket.com/event/btc-updown-5m-1788107700) via the
 * public Gamma API and exposes it as a {@link PolymarketMarketSnapshot}.
 *
 * CONFIRMED against live behavior (previously flagged as unverified):
 * - The event slug is "btc-updown-5m-{unixEpochSecondsOfWindowSTART}" — the
 *   timestamp is the window's OPEN time (aligned to a 300s boundary), NOT
 *   the close time. Using the close-time epoch here previously resolved to
 *   the wrong (next) window's slug, which never matches a real event.
 * - Gamma resolves slugs via GET /events?slug={slug} (query parameter,
 *   response is a JSON ARRAY of events), not /events/slug/{slug} (path
 *   parameter, single object). The path-style call 404s.
 * - Gamma's outcomes/outcomePrices/clobTokenIds arrays are ordered
 *   [Up, Down] for this market — still worth spot-checking a live payload
 *   if EV/side selection ever looks inverted, since Gamma has been known to
 *   vary field encoding (see parseJsonDecimalArray below).
 * - market.outcomePrices is effectively the LAST EXECUTED TRADE price, not
 *   a live order-book price — on a thin/short-dated market like this it can
 *   sit static for tens of seconds and then jump when a trade finally
 *   prints. Gamma's market object separately exposes top-level bestBid/
 *   bestAsk fields (plain numbers, not JSON-string-encoded like
 *   outcomePrices) which track the live order book and move continuously.
 *   We now prefer bestBid/bestAsk midpoint, falling back to
 *   lastTradePrice, and only falling back further to outcomePrices if
 *   neither is present — see resolveMarketPrices below.
 * - This market type has no fixed strike price — it resolves on whether the
 *   price at close is above or below the price at window open. We
 *   approximate the open price with our own blended feed's avg60sPrice the
 *   moment we detect a new window (slug change), which is itself only an
 *   approximation of whatever Chainlink price Polymarket actually anchors
 *   the window to.
 */
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

    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicReference<BigDecimal> referenceOpenPrice = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot = new AtomicReference<>();

    // De-duplication state for the SNAPSHOT_OK log line.
    private final AtomicReference<String> lastLoggedSignature = new AtomicReference<>();
    private final AtomicReference<Instant> lastSnapshotHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    public PolymarketDataProvider(@Qualifier("gammaWebClient") WebClient gammaWebClient,
                                  ObjectMapper objectMapper,
                                  PolymarketProperties properties,
                                  TaskScheduler liveDataTaskScheduler,
                                  Prices prices) {
        this.gammaWebClient = gammaWebClient;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
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
                log.info("REFRESH result=NO_EVENT slug={} detail='Gamma returned no event for this slug " +
                        "(check gammaBaseUrl, slug format, and whether the market is pre-listed yet)'", slug);
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

            JsonNode endDateNode = market.get("endDate");
            long endEpochMillis = endDateNode != null
                    ? Instant.parse(endDateNode.stringValue()).toEpochMilli()
                    : System.currentTimeMillis();
            long secondsUntilClose = Math.max(0, (endEpochMillis - System.currentTimeMillis()) / 1000);

            String previousSlug = currentSlug.getAndSet(slug);
            if (!slug.equals(previousSlug)) {
                referenceOpenPrice.set(null);
                log.info("REFRESH result=NEW_WINDOW slug={} secondsUntilClose={}", slug, secondsUntilClose);
            }

            BigDecimal strikePriceUsd = referenceOpenPrice.get();
            if (strikePriceUsd == null) {
                BigDecimal openPrice = prices.getAvg60sPrice() != null ? prices.getAvg60sPrice() : prices.getAvgPrice();
                if (openPrice != null) {
                    referenceOpenPrice.set(openPrice);
                    strikePriceUsd = openPrice;
                    log.info("REFRESH result=REFERENCE_PRICE_CAPTURED slug={} referenceOpenPrice={}", slug, openPrice);
                } else {
                    log.info("REFRESH result=NO_REFERENCE_PRICE_YET slug={} avgPrice={} avg60sPrice={} " +
                                    "detail='no provider has delivered a price tick yet — check exchange websocket connection logs'",
                            slug, prices.getAvgPrice(), prices.getAvg60sPrice());
                    latestSnapshot.set(null);
                    return;
                }
            }

            latestSnapshot.set(new PolymarketMarketSnapshot(slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd));
            logSnapshot(slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd);
        } catch (Exception e) {
            log.error("REFRESH result=ERROR detail='exception during Polymarket market refresh'", e);
            latestSnapshot.set(null);
        }
    }

    /**
     * Logs the SNAPSHOT_OK result at INFO only when upPrice/downPrice/
     * strikePriceUsd actually changed since the last log, or every
     * SNAPSHOT_HEARTBEAT_INTERVAL regardless — otherwise DEBUG.
     *
     * Gamma's outcomePrices field on /events is a slower, sometimes
     * cached mid/last-trade price rather than a live CLOB order-book
     * price, so it can legitimately sit unchanged for many refresh
     * cycles even while the real market is moving. This makes that
     * distinction visible: "changed=false" + a shrinking heartbeat
     * cadence means "still polling successfully, Gamma just hasn't
     * updated its cached price," not "stuck/broken."
     */
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
     * ASSUMPTION (unchanged from before): the single top-level
     * bestBid/bestAsk/lastTradePrice fields, and index 0 of
     * outcomePrices, all refer to the "Up" outcome; "Down" is its
     * complement (1 - price). Still worth spot-checking a live payload
     * if EV/side selection ever looks inverted.
     *
     * Returns null (and logs the reason) if no usable price could be
     * resolved at all.
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
}