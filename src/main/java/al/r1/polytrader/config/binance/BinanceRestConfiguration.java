package al.r1.polytrader.config.binance;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class BinanceRestConfiguration {

    @Bean
    public WebClient binanceWebClient(BinanceProperties properties) {
        return WebClient.builder()
                .baseUrl(properties.baseUrl())
                .build();
    }
}