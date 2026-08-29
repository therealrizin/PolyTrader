package al.r1.polytrader.config.services;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services")
public record ServicesWssProperties(
        String coinbase,
        String kraken,
        String bybit,
        String okx
) {}