package al.r1.polytrader.config.binance;

import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

public class BinanceRestConfiguration {

    @Bean
    public WebClient binanceWebClient(BinanceProperties properties) {
        return WebClient.builder()
                .baseUrl(properties.baseUrl())
                .build();
    }
}
