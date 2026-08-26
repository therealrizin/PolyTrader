package al.r1.polytrader.services.binance;

import al.r1.polytrader.services.binance.model.BinanceKline;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
public class BinanceService {

    private final WebClient webClient;


    public BinanceService(WebClient webClient) {
        this.webClient = webClient;
    }

    public List<BinanceKline> getKlines(
            String symbol,
            long startTime,
            long endTime
    ) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/v3/klines")
                        .queryParam("symbol", symbol)
                        .queryParam("interval", "1s")
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .queryParam("limit", 1000)
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<BinanceKline>>() {})
                .block();
    }


}
