package al.r1.polytrader.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class ExecutionWebClientConfig {

    @Bean
    public WebClient executionWebClient(
            @Value("${services.polymarket.execution-url}")
            String executionUrl,

            @Value("${services.polymarket.execution-token}")
            String executionToken
    ) {

        return WebClient.builder()
                .baseUrl(executionUrl)
                .defaultHeader(
                        "X-Executor-Token",
                        executionToken
                )
                .build();
    }
}