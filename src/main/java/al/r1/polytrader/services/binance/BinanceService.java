package al.r1.polytrader.services.binance;

import al.r1.polytrader.config.binance.BinanceProperties;
import al.r1.polytrader.services.CurrencyPairs;
import al.r1.polytrader.services.binance.model.BinanceKline;
import al.r1.polytrader.services.binance.model.BinanceTradeEvent;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BinanceService {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private BinanceProperties binanceProperties;

    @Getter
    @Setter
    private volatile Map<CurrencyPairs, BigDecimal> latestPrice;

    public BinanceService(WebClient webClient, ObjectMapper objectMapper, BinanceProperties binanceProperties) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.binanceProperties = binanceProperties;
        this.latestPrice = new HashMap<>();
    }

    @PostConstruct
    public void start() {
        connectPriceStream(CurrencyPairs.BTCUSD);
    }

    public void connectPriceStream(CurrencyPairs symbol) {

        WebSocketClient client = new StandardWebSocketClient();

        String url = binanceProperties.wssUrl() + symbol.getValue().toLowerCase() + "@trade";

        client.execute(
                new TextWebSocketHandler() {

                    @Override
                    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                        try {
                            BinanceTradeEvent event = objectMapper.readValue(message.getPayload(),
                                    BinanceTradeEvent.class);

                            latestPrice.put(symbol, new BigDecimal(event.p()));

                        } catch (Exception e) {
                            log.error(
                                    "Failed to process Binance message", e
                            );
                        }
                    }
                }
                , url);
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
