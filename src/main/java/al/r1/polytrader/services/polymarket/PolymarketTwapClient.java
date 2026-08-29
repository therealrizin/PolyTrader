package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.services.model.Prices;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes Chainlink 60s TWAP prices for BTC/USD via Polymarket RTDS.
 * Uses the configured wss_live_data_url from application.yaml.
 */
@Slf4j
@Service
public class PolymarketTwapClient {

    private static final Duration PING_INTERVAL = Duration.ofSeconds(5);  // required by RTDS
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    private final PolymarketProperties properties;
    private final Prices prices;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;

    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private volatile boolean running = false;

    public PolymarketTwapClient(PolymarketProperties properties,
                                Prices prices,
                                ObjectMapper objectMapper,
                                TaskScheduler liveDataTaskScheduler) {
        this.properties = properties;
        this.prices = prices;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        connect();
        taskScheduler.scheduleAtFixedRate(this::sendPing, PING_INTERVAL);
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        WebSocketSession session = currentSession.getAndSet(null);
        if (session != null && session.isOpen()) {
            try { session.close(CloseStatus.NORMAL); } catch (Exception ignored) { }
        }
    }

    private void connect() {
        if (!running) return;

        WebSocketClient client = new StandardWebSocketClient();
        String wsUrl = properties.wssLiveDataUrl();  // from config

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                log.info("Connected to Polymarket RTDS at {}", wsUrl);

                try {
                    // Subscribe to 60s TWAP for BTC/USD
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "action", "subscribe",
                            "subscriptions", List.of(
                                    Map.of(
                                            "topic", "crypto_prices_twap_sixty",
                                            "type", "update",
                                            "filters", "{\"symbol\":\"btc/usd\"}"
                                    )
                            )
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                    log.info("Subscribed to crypto_prices_twap_sixty for btc/usd");
                } catch (Exception e) {
                    log.error("Failed to send RTDS subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                String payload = message.getPayload();
                if ("PONG".equals(payload) || "PING".equals(payload)) return;
                try {
                    onTwapUpdate(objectMapper.readTree(payload));
                } catch (Exception e) {
                    log.error("Failed to process RTDS message", e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("RTDS transport error, reconnecting", exception);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("RTDS connection closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, wsUrl);
    }

    private void scheduleReconnect() {
        if (!running) return;
        taskScheduler.schedule(this::connect, Instant.now().plus(RECONNECT_DELAY));
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage("PING"));
            } catch (Exception e) {
                log.warn("Failed to send RTDS PING", e);
            }
        }
    }

    private void onTwapUpdate(JsonNode node) {
        JsonNode topicNode = node.get("topic");
        if (topicNode == null) return;
        if (!"crypto_prices_twap_sixty".equals(topicNode.asText())) return;

        JsonNode payload = node.get("payload");
        if (payload == null) return;

        JsonNode valueNode = payload.get("value");
        if (valueNode == null) return;

        BigDecimal twapPrice = new BigDecimal(valueNode.asText());
        log.debug("RTDS 60s TWAP BTC/USD: {}", twapPrice);

        // Store in global Prices
        prices.setPolymarketPrice(twapPrice);
    }
}