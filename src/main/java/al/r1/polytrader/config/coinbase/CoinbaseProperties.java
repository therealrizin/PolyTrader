// config/coinbase/CoinbaseProperties.java
package al.r1.polytrader.config.coinbase;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services.coinbase")
public record CoinbaseProperties(String wssUrl) { }