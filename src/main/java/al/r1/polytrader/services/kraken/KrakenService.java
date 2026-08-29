package al.r1.polytrader.services.kraken;

import al.r1.polytrader.config.services.ServicesWssProperties;
import al.r1.polytrader.services.model.CurrencyPairs;
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
 * Live BTC/USD trades from Kraken's public WebSocket API v2
 * (https://docs.kraken.com/api/docs/websocket-v2/trade). Kraken's v1 API is
 * deprecated; this targets v2's wss://ws.kraken.com/v2 endpoint and message
 * shape, which is not backward compatible with v1.
 *
 * Kraken sends an automatic "heartbeat" channel message ~once per second
 * once subscribed to any channel, but we don't rely on that for liveness —
 * the same staleness watchdog pattern as BinanceService/CoinbaseService is
 * used instead, so all three providers fail the same way and are equally
 * observable.
 */
@Slf4j
@Service
public class KrakenService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(15);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final String SYMBOL = "BTC/USD";

    private final ServicesWssProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler liveDataTaskScheduler;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    private volatile long lastMessageAtMillis = -1;
    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();

    // Held for the connection's lifetime — see BinanceService for why an
    // unreferenced WebSocketClient can let the underlying connection die
    // without a corresponding close callback.
    private volatile WebSocketClient client;

    public KrakenService(ServicesWssProperties properties,
                         ObjectMapper objectMapper,
                         TaskScheduler liveDataTaskScheduler) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
    }

    @PostConstruct
    public void start() {
        connect();
        liveDataTaskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
    }

    public void connect() {
        client = new StandardWebSocketClient();
        String url = properties.kraken();

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                lastMessageAtMillis = System.currentTimeMillis();
                log.info("Connected to Kraken trade stream for {}", SYMBOL);

                try {
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "method", "subscribe",
                            "params", Map.of(
                                    "channel", "trade",
                                    "symbol", List.of(SYMBOL)
                            )
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                    log.info("Subscribed to Kraken trade channel for {}", SYMBOL);
                } catch (Exception e) {
                    log.error("Failed to send Kraken subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                try {
                    onMessage(objectMapper.readTree(message.getPayload()));
                } catch (Exception e) {
                    log.error("Failed to process Kraken message: {}", message.getPayload(), e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Kraken websocket transport error, reconnecting", exception);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Kraken websocket closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, url);
    }

    private void onMessage(JsonNode node) {
        JsonNode channelNode = node.get("channel");
        // Ignore subscribe acks, heartbeat, and status messages — only the
        // "trade" channel carries prices.
        if (channelNode == null || !"trade".equals(channelNode.stringValue())) {
            return;
        }

        JsonNode data = node.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) return;

        // A message can batch multiple trades; the last one is most recent.
        JsonNode lastTrade = data.get(data.size() - 1);
        JsonNode priceNode = lastTrade.get("price");
        if (priceNode == null) return;

        latestPrice.put(CurrencyPairs.BTCUSD, priceNode.decimalValue());
        lastMessageAtMillis = System.currentTimeMillis();
    }

    /**
     * Backstop for disconnects the transport callbacks miss (see
     * BinanceService for the same pattern and rationale).
     */
    private void checkStaleness() {
        if (lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No Kraken trade messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale Kraken session", e);
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