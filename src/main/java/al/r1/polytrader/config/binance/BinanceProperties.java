package al.r1.polytrader.config.model;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "binance")
public record BinanceProperties(
        String baseUrl,
        String wssUrl,
        String apiKey,
        String secret
) { }
