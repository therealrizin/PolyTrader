package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
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
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class ChainlinkPriceStreamClient {

    private static final Duration PING_INTERVAL = Duration.ofSeconds(5); // required by RTDS
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(20);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);

    private static final String RAW_TOPIC = "crypto_prices_chainlink";
    private static final String TWAP_SIXTY_TOPIC = "crypto_prices_twap_sixty";
    private static final BigInteger E18 = BigInteger.TEN.pow(18);

    private final PolymarketProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;
    private final Prices prices;
    private final ProbabilityTable probabilityTable;

    private final PolymarketRollingWindow btcRollingWindow = new PolymarketRollingWindow();
    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();

    private volatile WebSocketClient client;
    private volatile long lastMessageAtMillis = -1;
    private volatile boolean running = false;

    public ChainlinkPriceStreamClient(PolymarketProperties properties,
                                      ObjectMapper objectMapper,
                                      TaskScheduler liveDataTaskScheduler,
                                      Prices prices,
                                      ProbabilityTable probabilityTable) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;
        this.prices = prices;
        this.probabilityTable = probabilityTable;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        connect();
        taskScheduler.scheduleAtFixedRate(this::sendPing, PING_INTERVAL);
        taskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
        log.info("ChainlinkPriceStreamClient started for symbols {}", (Object) ChainlinkSymbol.values());
    }

    public synchronized void stop() {
        running = false;
        WebSocketSession session = currentSession.getAndSet(null);
        if (session != null && session.isOpen()) {
            try {
                session.close(CloseStatus.NORMAL);
            } catch (Exception ignored) {
            }
        }
    }

    private void connect() {
        if (!running) return;

        client = new StandardWebSocketClient(); // strong reference held for connection lifetime
        String url = properties.wssLiveDataUrl();

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                lastMessageAtMillis = System.currentTimeMillis();
                log.info("Connected to Polymarket RTDS Chainlink price stream at {}", url);
                sendSubscription(session);
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                String payload = message.getPayload();
                if ("PONG".equals(payload) || "PING".equals(payload)) return;
                lastMessageAtMillis = System.currentTimeMillis();
                try {
                    onMessage(objectMapper.readTree(payload));
                } catch (Exception e) {
                    log.error("Failed to process Chainlink RTDS message: {}", payload, e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Chainlink RTDS transport error, reconnecting", exception);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Chainlink RTDS connection closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                if (running) scheduleReconnect();
            }
        }, url);
    }

    private void sendSubscription(WebSocketSession session) {
        try {
            List<Map<String, Object>> subscriptions = new ArrayList<>();
            for (ChainlinkSymbol symbol : ChainlinkSymbol.values()) {
                String filter = "{\"symbol\":\"" + symbol.getWire() + "\"}";
                subscriptions.add(Map.of("topic", RAW_TOPIC, "type", "*", "filters", filter));
                subscriptions.add(Map.of("topic", TWAP_SIXTY_TOPIC, "type", "update", "filters", filter));
            }
            String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                    "action", "subscribe",
                    "subscriptions", subscriptions
            ));
            session.sendMessage(new TextMessage(subscribeMsg));
            log.info("Subscribed to Chainlink raw + 60s TWAP prices for {} symbols", ChainlinkSymbol.values().length);
        } catch (Exception e) {
            log.error("Failed to send Chainlink RTDS subscribe message", e);
        }
    }

    private void onMessage(JsonNode node) {
        String topic = node.path("topic").asText("");
        if (topic.isBlank()) return; // heartbeat/ack frames etc.

        JsonNode payload = node.get("payload");
        if (payload == null) return;

        ChainlinkSymbol symbol = ChainlinkSymbol.fromWire(payload.path("symbol").asText(null));
        if (symbol == null) return;

        switch (topic) {
            case RAW_TOPIC -> handleRawPrice(symbol, payload);
            case TWAP_SIXTY_TOPIC -> handleTwapSixty(symbol, payload);
            default -> log.debug("Unhandled Chainlink RTDS topic={}: {}", topic, node);
        }
    }

    private void handleRawPrice(
            ChainlinkSymbol symbol,
            JsonNode payload
    ) {
        BigDecimal price = parseDecimal(payload.get("value"));

        if (price == null) {
            return;
        }

        JsonNode timestampNode = payload.get("timestamp");

        if (timestampNode == null || timestampNode.isNull()) {
            log.warn(
                    "Chainlink raw price missing payload.timestamp: symbol={} payload={}",
                    symbol.getWire(),
                    payload
            );

            /*
             * We still update the current price, but do not put it into the
             * historical resolution buffer because we don't know exactly
             * when Polymarket observed it.
             */
            prices.updatePrice(symbol, price);
            return;
        }

        long observedAtMillis = timestampNode.longValue();

        /*
         * Some APIs provide seconds while others provide milliseconds.
         * Normalize defensively.
         */
        if (observedAtMillis > 0 && observedAtMillis < 100_000_000_000L) {
            observedAtMillis *= 1000L;
        }

        prices.updatePrice(
                symbol,
                price,
                observedAtMillis
        );

        log.trace(
                "Chainlink raw price {}={} observedAt={}",
                symbol.getWire(),
                price,
                Instant.ofEpochMilli(observedAtMillis)
        );
    }

    private void handleTwapSixty(ChainlinkSymbol symbol, JsonNode payload) {
        BigDecimal twap = parseTwapValue(payload);
        if (twap == null) return;
        prices.updateAvg60sPrice(symbol, twap);
        log.trace("Chainlink 60s TWAP {}={}", symbol.getWire(), twap);

        if (symbol == ChainlinkSymbol.BTC_USD) {
            JsonNode obsTimestampNode = payload.get("timestamp");
            if (obsTimestampNode == null) {
                log.warn("BTC 60s TWAP update missing payload.timestamp; skipping probability table update");
                return;
            }
            long observedAtMillis = obsTimestampNode.longValue();
            btcRollingWindow.addAndUpdateTable(observedAtMillis, twap.doubleValue(), probabilityTable);
        }
    }

    /**
     * Prefers the exact E18 fixed-point {@code full_accuracy_value} over the
     * display-only {@code value}, per the chainlink-twap docs.
     */
    private BigDecimal parseTwapValue(JsonNode payload) {
        JsonNode fullAccuracyNode = payload.get("full_accuracy_value");
        if (fullAccuracyNode != null && !fullAccuracyNode.isNull()) {
            try {
                BigInteger raw = new BigInteger(fullAccuracyNode.stringValue());
                return new BigDecimal(raw).divide(new BigDecimal(E18));
            } catch (Exception e) {
                log.warn("Failed to parse full_accuracy_value '{}', falling back to value", fullAccuracyNode);
            }
        }
        return parseDecimal(payload.get("value"));
    }

    private BigDecimal parseDecimal(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            return node.isNumber() ? node.decimalValue() : new BigDecimal(node.stringValue());
        } catch (Exception e) {
            log.debug("Failed to parse numeric field '{}'", node, e);
            return null;
        }
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage("PING"));
            } catch (Exception e) {
                log.warn("Failed to send Chainlink RTDS PING", e);
            }
        }
    }

    private void checkStaleness() {
        if (!running || lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No Chainlink RTDS messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale Chainlink RTDS session", e);
                }
            }

            lastMessageAtMillis = System.currentTimeMillis();
            connect();
        }
    }

    private void scheduleReconnect() {
        if (!running) return;
        taskScheduler.schedule(this::connect, Instant.now().plus(RECONNECT_DELAY));
    }
}