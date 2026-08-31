package al.r1.polytrader.config.polymarket;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
@EnableConfigurationProperties(PolymarketProperties.class)
public class PolymarketConfiguration {

    @Bean
    public WebClient gammaWebClient(PolymarketProperties properties) {
        return WebClient.builder()
                .baseUrl(properties.gammaBaseUrl())
                .defaultHeader("User-Agent", "PolyTrader/1.0")
                .build();
    }
}