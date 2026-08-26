package al.r1.polytrader.config.binance;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(BinanceProperties.class)
public class BinanceConfiguration {

}
