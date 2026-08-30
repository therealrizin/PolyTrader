// config/coinbase/CoinbaseConfiguration.java
package al.r1.polytrader.config.kraken;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KrakenProperties.class)
public class KrakenConfiguration { }