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
 * UNVERIFIED ASSUMPTIONS — confirm against a live call before trusting this
 * in anything but a research/backtest context:
 * - The event slug is "btc-updown-5m-{unixEpochSecondsOfWindowClose}",
 *   inferred from the example URL and Polymarket's other recurring
 *   "updown" markets.
 * - Gamma's outcomes/outcomePrices/clobTokenIds arrays are ordered
 *   [Up, Down] for this market. If that's wrong, every probability/EV
 *   comparison downstream silently inverts sign.
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

    private final WebClient gammaWebClient;
    private final ObjectMapper objectMapper;
    private final PolymarketProperties properties;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;

    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicReference<BigDecimal> referenceOpenPrice = new AtomicReference<>();
    private final AtomicReference<PolymarketMarketSnapshot> latestSnapshot = new AtomicReference<>();

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
                log.debug("No Polymarket event found for slug {}", slug);
                return;
            }

            JsonNode marketsNode = event.get("markets");
            if (marketsNode == null || !marketsNode.isArray() || marketsNode.isEmpty()) {
                log.warn("Polymarket event {} returned no markets", slug);
                return;
            }
            JsonNode market = marketsNode.get(0);

            List<BigDecimal> outcomePrices = parseJsonDecimalArray(market.get("outcomePrices"));
            if (outcomePrices.size() < 2) {
                log.warn("Polymarket market {} outcomePrices missing/incomplete: {}", slug, outcomePrices);
                return;
            }

            // ASSUMPTION: index 0 = Up, index 1 = Down — see class javadoc.
            BigDecimal upPrice = outcomePrices.get(0);
            BigDecimal downPrice = outcomePrices.get(1);

            JsonNode endDateNode = market.get("endDate");
            long endEpochMillis = endDateNode != null
                    ? Instant.parse(endDateNode.stringValue()).toEpochMilli()
                    : System.currentTimeMillis();
            long secondsUntilClose = Math.max(0, (endEpochMillis - System.currentTimeMillis()) / 1000);

            String previousSlug = currentSlug.getAndSet(slug);
            BigDecimal strikePriceUsd;
            if (!slug.equals(previousSlug)) {
                BigDecimal openPrice = prices.getAvg60sPrice() != null ? prices.getAvg60sPrice() : prices.getAvgPrice();
                referenceOpenPrice.set(openPrice);
                strikePriceUsd = openPrice;
                log.info("New Polymarket window {} detected, reference open price = {}", slug, openPrice);
            } else {
                strikePriceUsd = referenceOpenPrice.get();
            }

            if (strikePriceUsd == null) {
                log.debug("No reference price available yet for {}, skipping snapshot", slug);
                return;
            }

            latestSnapshot.set(new PolymarketMarketSnapshot(slug, upPrice, downPrice, secondsUntilClose, strikePriceUsd));
        } catch (Exception e) {
            log.error("Failed to refresh Polymarket market snapshot", e);
        }
    }

    private String computeCurrentSlug() {
        long nowSeconds = System.currentTimeMillis() / 1000;
        long windowSeconds = WINDOW.toSeconds();
        long windowEnd = ((nowSeconds / windowSeconds) + 1) * windowSeconds;
        return SLUG_PREFIX + windowEnd;
    }

    private JsonNode fetchEventBySlug(String slug) {
        return gammaWebClient.get()
                .uri("/events/slug/{slug}", slug)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> {
                    log.debug("Gamma event lookup failed for slug {}: {}", slug, e.toString());
                    return Mono.empty();
                })
                .block();
    }

    private List<BigDecimal> parseJsonDecimalArray(JsonNode node) {
        if (node == null) return List.of();
        try {
            JsonNode arr = objectMapper.readTree(node.stringValue());
            List<BigDecimal> result = new ArrayList<>();
            arr.forEach(n -> result.add(new BigDecimal(n.stringValue())));
            return result;
        } catch (Exception e) {
            log.warn("Failed to parse Gamma JSON-array field '{}'", node, e);
            return List.of();
        }
    }
}