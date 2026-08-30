// config/coinbase/CoinbaseProperties.java
package al.r1.polytrader.config.okx;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services.okx")
public record OkxProperties(String wssUrl) { }