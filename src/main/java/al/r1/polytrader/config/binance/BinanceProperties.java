package al.r1.polytrader.config.binance;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services.binance")
public record BinanceProperties(
        String baseUrl,
        String wssUrl,
        String apiKey,
        String secret
) { }
