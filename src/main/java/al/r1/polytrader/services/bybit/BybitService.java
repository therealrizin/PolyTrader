package al.r1.polytrader.services.bybit;

import al.r1.polytrader.config.bybit.BybitProperties;
import al.r1.polytrader.services.model.CurrencyPairs;
import al.r1.polytrader.services.model.PriceTickAggregators;
import al.r1.polytrader.services.model.Prices;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Live BTC/USDT trades from Bybit's public v5 WebSocket API
 * (https://bybit-exchange.github.io/docs/v5/websocket/public/trade).
 * Follows the same hardened connection pattern as the other providers: a
 * strong class-level reference to the WebSocketClient/session plus an
 * independent staleness watchdog, since Bybit's own ping/pong keepalive
 * gives no stronger liveness guarantee than the transport callbacks do.
 */
@Slf4j
@Service
public class BybitService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(15);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration PING_INTERVAL = Duration.ofSeconds(20); // Bybit recommends pinging every 20s
    private static final String SYMBOL = "BTCUSDT";
    private static final String TOPIC = "publicTrade." + SYMBOL;

    private final BybitProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PriceTickAggregators tickAggregators;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    private volatile long lastMessageAtMillis = -1;
    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();

    private volatile WebSocketClient client;

    public BybitService(BybitProperties properties,
                        ObjectMapper objectMapper,
                        TaskScheduler liveDataTaskScheduler,
                        Prices prices,
                        PriceTickAggregators tickAggregators) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.tickAggregators = tickAggregators;
    }

    @PostConstruct
    public void start() {
        connect();
        liveDataTaskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
        liveDataTaskScheduler.scheduleAtFixedRate(this::sendPing, PING_INTERVAL);
    }

    public void connect() {
        client = new StandardWebSocketClient();
        String url = properties.wssUrl();

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                lastMessageAtMillis = System.currentTimeMillis();
                log.info("Connected to Bybit trade stream for {}", SYMBOL);

                try {
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "op", "subscribe",
                            "args", List.of(TOPIC)
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                    log.info("Subscribed to Bybit topic {}", TOPIC);
                } catch (Exception e) {
                    log.error("Failed to send Bybit subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                try {
                    onMessage(objectMapper.readTree(message.getPayload()));
                } catch (Exception e) {
                    log.error("Failed to process Bybit message: {}", message.getPayload(), e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Bybit websocket transport error, reconnecting", exception);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Bybit websocket closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, url);
    }

    private void onMessage(JsonNode node) {
        JsonNode topicNode = node.get("topic");
        // Ignore subscribe acks and pong replies — only the publicTrade
        // topic carries prices.
        if (topicNode == null || !TOPIC.equals(topicNode.stringValue())) {
            return;
        }

        JsonNode data = node.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) return;

        // A message can batch multiple trades; the last one is most recent.
        JsonNode lastTrade = data.get(data.size() - 1);
        JsonNode priceNode = lastTrade.get("p");
        if (priceNode == null) return;

        try {
            BigDecimal price = new BigDecimal(priceNode.stringValue());
            latestPrice.put(CurrencyPairs.BTCUSD, price);

            prices.setBybitPrice(price);
            tickAggregators.getBybit().record(price);

            lastMessageAtMillis = System.currentTimeMillis();
        } catch (Exception e) {
            log.warn("Failed to parse Bybit trade price '{}'", priceNode, e);
        }
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage(objectMapper.writeValueAsString(Map.of("op", "ping"))));
            } catch (Exception e) {
                log.warn("Failed to send Bybit ping", e);
            }
        }
    }

    /**
     * Backstop for disconnects the transport callbacks miss (see
     * BinanceService for the same pattern and rationale).
     */
    private void checkStaleness() {
        if (lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No Bybit trade messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale Bybit session", e);
                }
            }

            lastMessageAtMillis = System.currentTimeMillis();
            connect();
        }
    }

    private void scheduleReconnect() {
        liveDataTaskScheduler.schedule(this::connect, Instant.now().plus(RECONNECT_DELAY));
    }
}