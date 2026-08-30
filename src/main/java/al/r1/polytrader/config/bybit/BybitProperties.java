// config/coinbase/CoinbaseProperties.java
package al.r1.polytrader.config.bybit;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "bybit")
public record BybitProperties(String wssUrl) { }