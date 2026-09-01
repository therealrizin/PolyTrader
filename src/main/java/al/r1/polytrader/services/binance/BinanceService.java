package al.r1.polytrader.services.binance;

import al.r1.polytrader.services.model.CurrencyPairs;
import al.r1.polytrader.services.model.PriceTickAggregators;
import al.r1.polytrader.services.model.Prices;
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
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class BinanceService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    // BTCUSDT trades multiple times per second on Binance in normal
    // conditions. If we go this long with no message on an ostensibly-open
    // session, treat it as dead rather than trust the transport-level
    // callbacks, which don't always fire for a half-open socket or a
    // client that's been silently torn down by the underlying JSR-356
    // implementation.
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(10);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(3);

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PriceTickAggregators tickAggregators;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    private final Map<CurrencyPairs, Long> lastMessageAtMillis = new ConcurrentHashMap<>();
    private final Map<CurrencyPairs, AtomicReference<WebSocketSession>> sessions = new ConcurrentHashMap<>();

    // IMPORTANT: the JSR-356 client implementation backing
    // StandardWebSocketClient (Tyrus) can tear a connection down without
    // firing afterConnectionClosed/handleTransportError if nothing holds a
    // strong reference to the WebSocketClient itself for the life of the
    // connection. Previously this was a local variable in
    // connectPriceStream() and became eligible for GC as soon as the method
    // returned — keeping it here fixes silent, un-notified disconnects.
    private final Map<CurrencyPairs, WebSocketClient> clients = new ConcurrentHashMap<>();

    public BinanceService(@Qualifier("binanceWebClient") WebClient webClient, ObjectMapper objectMapper,
                          TaskScheduler liveDataTaskScheduler,
                          Prices prices,
                          PriceTickAggregators tickAggregators) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.tickAggregators = tickAggregators;
    }

    @PostConstruct
    public void start() {
        connectPriceStream(CurrencyPairs.BTCUSDT);
        liveDataTaskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
    }

    public void connectPriceStream(CurrencyPairs symbol) {
        WebSocketClient client = new StandardWebSocketClient();
        clients.put(symbol, client); // hold a strong reference for the connection's lifetime

        String url = binanceProperties.wssUrl() + symbol.getValue().toLowerCase() + "@trade";

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                sessions.computeIfAbsent(symbol, s -> new AtomicReference<>()).set(session);
                lastMessageAtMillis.put(symbol, System.currentTimeMillis());
                log.info("Connected to Binance trade stream for {}", symbol.getValue());
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                try {
                    BinanceTradeEvent event = objectMapper.readValue(message.getPayload(), BinanceTradeEvent.class);
                    BigDecimal price = new BigDecimal(event.p());
                    CurrencyPairs usdPair = CurrencyPairs.getUsdXValue(symbol);
                    latestPrice.put(usdPair, price);

                    if (usdPair == CurrencyPairs.BTCUSD) {
                        prices.setBinancePrice(price);
                        tickAggregators.getBinance().record(price);
                    }

                    lastMessageAtMillis.put(symbol, System.currentTimeMillis());
                    log.debug("Binance trade {} price={} tradeId={}", symbol.getValue(), price, event.t());
                } catch (Exception e) {
                    log.error("Failed to process Binance message for {}: {}", symbol.getValue(), message.getPayload(), e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Binance websocket transport error for {}, reconnecting", symbol.getValue(), exception);
                sessions.computeIfAbsent(symbol, s -> new AtomicReference<>()).compareAndSet(session, null);
                scheduleReconnect(symbol);
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Binance websocket closed for {} ({}), reconnecting", symbol.getValue(), closeStatus);
                sessions.computeIfAbsent(symbol, s -> new AtomicReference<>()).compareAndSet(session, null);
                scheduleReconnect(symbol);
            }
        }, url);
    }

    /**
     * Backstop for disconnects that the transport callbacks above don't
     * catch. Since BTCUSDT trades continuously, silence beyond
     * STALE_THRESHOLD on a pair we're actively subscribed to is itself the
     * failure signal.
     */
    private void checkStaleness() {
        long now = System.currentTimeMillis();
        for (CurrencyPairs symbol : lastMessageAtMillis.keySet()) {
            Long last = lastMessageAtMillis.get(symbol);
            if (last == null) continue;

            long silentFor = now - last;
            if (silentFor > STALE_THRESHOLD.toMillis()) {
                log.warn("No Binance trade messages for {} in {}ms, forcing reconnect", symbol.getValue(), silentFor);

                AtomicReference<WebSocketSession> ref = sessions.get(symbol);
                WebSocketSession session = (ref != null) ? ref.getAndSet(null) : null;
                if (session != null && session.isOpen()) {
                    try {
                        session.close(CloseStatus.GOING_AWAY);
                    } catch (Exception e) {
                        log.warn("Failed to close stale Binance session for {}", symbol.getValue(), e);
                    }
                }

                // Prevent this check from re-firing every few seconds while
                // the reconnect is in flight; afterConnectionEstablished resets it.
                lastMessageAtMillis.put(symbol, now);
                connectPriceStream(symbol);
            }
        }
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