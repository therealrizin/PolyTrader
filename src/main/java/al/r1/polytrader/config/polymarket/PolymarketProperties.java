package al.r1.polytrader.config.polymarket;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "polymarket")
public record PolymarketProperties(
        String wssMarketUrl,
        String gammaBaseUrl,
        int marketRefreshSeconds
) {}