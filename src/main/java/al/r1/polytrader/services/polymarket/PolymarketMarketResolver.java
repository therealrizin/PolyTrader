package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.services.polymarket.model.GammaMarketDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Resolves the CLOB token IDs of the currently active Polymarket 5-minute
 * BTC "Up or Down" market.
 *
 * Discovery is deterministic, not searched: the 5m BTC series mints one
 * market per 300s boundary with slug "btc-updown-5m-<windowStartEpochSeconds>".
 * This depends on that slug convention staying stable; if Polymarket changes
 * it, resolveCurrentMarket() starts returning empty and logs a warning rather
 * than silently going stale.
 *
 * Extension point: only BTC/5m is wired up. Other assets or intervals (ETH,
 * 15m, etc.) reuse this same class by parameterizing the prefix/interval used
 * to build the slug — nothing else about the resolver is BTC-specific.
 */
@Slf4j
@Component
public class PolymarketMarketResolver {

    private static final String ASSET_PREFIX = "btc-updown";
    private static final int WINDOW_SECONDS = 300;

    private final WebClient gammaWebClient;
    private final ObjectMapper objectMapper;

    public PolymarketMarketResolver(@Qualifier("gammaWebClient") WebClient gammaWebClient,
                                    ObjectMapper objectMapper) {
        this.gammaWebClient = gammaWebClient;
        this.objectMapper = objectMapper;
    }

    public record ResolvedMarket(String slug, String upTokenId, String downTokenId, String resolutionSource) {}

    public long currentWindowStartEpochSeconds() {
        long now = Instant.now().getEpochSecond();
        return (now / WINDOW_SECONDS) * WINDOW_SECONDS;
    }

    public String slugForWindow(long windowStartEpochSeconds) {
        return ASSET_PREFIX + "-5m-" + windowStartEpochSeconds;
    }

    public Optional<ResolvedMarket> resolveCurrentMarket() {
        String slug = slugForWindow(currentWindowStartEpochSeconds());
        try {
            List<GammaMarketDto> markets = gammaWebClient.get()
                    .uri(uriBuilder -> uriBuilder.path("/markets").queryParam("slug", slug).build())
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<List<GammaMarketDto>>() {})
                    .block();

            if (markets == null || markets.isEmpty()) {
                log.warn("No Gamma market found for slug {} (not listed yet, or slug format changed)", slug);
                return Optional.empty();
            }

            GammaMarketDto market = markets.get(0);
            if (market.clobTokenIds() == null) {
                log.warn("Gamma market {} has no clobTokenIds", slug);
                return Optional.empty();
            }

            List<String> tokenIds = objectMapper.readValue(market.clobTokenIds(), List.class);
            if (tokenIds.size() < 2) {
                log.warn("Gamma market {} clobTokenIds malformed: {}", slug, tokenIds);
                return Optional.empty();
            }

            // Assumed outcome order [Up, Down] matching clobTokenIds order.
            // Single point of change if that assumption ever proves wrong.
            return Optional.of(new ResolvedMarket(slug, tokenIds.get(0), tokenIds.get(1), market.resolutionSource()));

        } catch (Exception e) {
            log.error("Failed to resolve Polymarket market for slug {}", slug, e);
            return Optional.empty();
        }
    }
}
