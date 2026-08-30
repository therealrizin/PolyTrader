// config/coinbase/CoinbaseConfiguration.java
package al.r1.polytrader.config.okx;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OkxProperties.class)
public class OkxConfiguration { }