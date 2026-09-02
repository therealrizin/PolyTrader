package al.r1.polytrader.config;

import al.r1.polytrader.config.model.TradingProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        TradingProperties.class
})
public class PropertiesEnable {
}