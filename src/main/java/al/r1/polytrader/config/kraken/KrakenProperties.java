// config/coinbase/CoinbaseProperties.java
package al.r1.polytrader.config.kraken;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services.kraken")
public record KrakenProperties(String wssUrl) { }