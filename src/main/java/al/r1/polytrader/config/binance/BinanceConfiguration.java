package al.r1.polytrader.config;

import al.r1.polytrader.config.model.BinanceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(BinanceProperties.class)
public class BinanceConfiguration {

}
