package al.r1.polytrader.config.polymarket;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services.polymarket")
public record PolymarketProperties(
        String wssLiveDataUrl,
        String gammaBaseUrl,
        String clobWssUrl,
        String marketWssUrl,
        String apiKey,
        String apiKeyAddress,
        int marketRefreshSeconds
) {}