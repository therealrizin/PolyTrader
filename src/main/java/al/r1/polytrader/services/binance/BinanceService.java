package al.r1.polytrader.services.binance;

import al.r1.polytrader.config.binance.BinanceProperties;
import al.r1.polytrader.services.model.CurrencyPairs;
import al.r1.polytrader.services.binance.model.BinanceKline;
import al.r1.polytrader.services.binance.model.BinanceTradeEvent;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class BinanceService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final BinanceProperties binanceProperties;
    private final TaskScheduler liveDataTaskScheduler;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    public BinanceService(@Qualifier("binanceWebClient") WebClient webClient, ObjectMapper objectMapper,
                          BinanceProperties binanceProperties,
                          TaskScheduler liveDataTaskScheduler) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.binanceProperties = binanceProperties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
    }

    @PostConstruct
    public void start() {
        connectPriceStream(CurrencyPairs.BTCUSDT);
    }

    public void connectPriceStream(CurrencyPairs symbol) {
        WebSocketClient client = new StandardWebSocketClient();
        String url = binanceProperties.wssUrl() + symbol.getValue().toLowerCase() + "@trade";

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                log.info("Connected to Binance trade stream for {}", symbol.getValue());
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                try {
                    BinanceTradeEvent event = objectMapper.readValue(message.getPayload(), BinanceTradeEvent.class);
                    latestPrice.put(CurrencyPairs.getUsdXValue(symbol), new BigDecimal(event.p()));
                } catch (Exception e) {
                    log.error("Failed to process Binance message", e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Binance websocket transport error for {}, reconnecting", symbol.getValue(), exception);
                scheduleReconnect(symbol);
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Binance websocket closed for {} ({}), reconnecting", symbol.getValue(), closeStatus);
                scheduleReconnect(symbol);
            }
        }, url);
    }

    private void scheduleReconnect(CurrencyPairs symbol) {
        liveDataTaskScheduler.schedule(() -> connectPriceStream(symbol), Instant.now().plus(RECONNECT_DELAY));
    }

    public List<BinanceKline> getKlines(String symbol, long startTime, long endTime) {
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
