// config/coinbase/CoinbaseConfiguration.java
package al.r1.polytrader.config.coinbase;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CoinbaseProperties.class)
public class CoinbaseConfiguration { }