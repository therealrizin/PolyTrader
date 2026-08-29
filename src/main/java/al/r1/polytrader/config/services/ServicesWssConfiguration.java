package al.r1.polytrader.config.services;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ServicesWssProperties.class)
public class ServicesWssConfiguration {
}