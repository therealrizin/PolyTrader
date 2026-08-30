// config/coinbase/CoinbaseConfiguration.java
package al.r1.polytrader.config.bybit;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(BybitProperties.class)
public class BybitConfiguration { }