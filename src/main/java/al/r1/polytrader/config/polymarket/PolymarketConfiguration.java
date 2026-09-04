package al.r1.polytrader.config.polymarket;

import io.netty.handler.timeout.ReadTimeoutHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties(PolymarketProperties.class)
public class PolymarketConfiguration {

    @Bean
    @Qualifier("gammaWebClient")
    public WebClient gammaWebClient(PolymarketProperties properties) {

        return WebClient.builder()
                .baseUrl(properties.gammaBaseUrl())
                .defaultHeader(HttpHeaders.USER_AGENT, "PolyTrader/1.0")
                .build();
    }

    @Bean
    @Qualifier("polymarketWebClient")
    public WebClient polymarketWebClient(PolymarketProperties properties) {

        HttpClient httpClient = HttpClient.create()
                .responseTimeout(Duration.ofSeconds(10))
                .httpResponseDecoder(spec -> spec.maxHeaderSize(16 * 1024))
                .doOnConnected(connection -> connection.addHandlerLast(new ReadTimeoutHandler(10)));

        return WebClient.builder()
                .baseUrl(properties.baseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer ->
                        configurer
                                .defaultCodecs()
                                .maxInMemorySize(2 * 1024 * 1024))
                .defaultHeader(HttpHeaders.USER_AGENT,
                        "Mozilla/5.0 (X11; Linux x86_64) " +
                                "AppleWebKit/537.36 " +
                                "(KHTML, like Gecko) " +
                                "Chrome/140.0.0.0 Safari/537.36")
                .defaultHeader(HttpHeaders.ACCEPT,
                        "text/html,application/xhtml+xml," +
                                "application/xml;q=0.9," +
                                "image/avif,image/webp,*/*;q=0.8")
                .defaultHeader(HttpHeaders.ACCEPT_LANGUAGE,
                        "en-US,en;q=0.9")
                .build();
    }
}