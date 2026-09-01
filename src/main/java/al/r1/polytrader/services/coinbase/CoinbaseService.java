package al.r1.polytrader.services.coinbase;

import al.r1.polytrader.config.coinbase.CoinbaseProperties;
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
 * Live BTC-USD ticker prices from Coinbase Exchange's public WebSocket feed
 * (https://docs.cdp.coinbase.com/exchange/websocket-feed/channels). Unlike
 * Binance/Kraken trade streams, "ticker" fires on every match for the
 * product, giving effectively the same update cadence.
 */
@Slf4j
@Service
public class CoinbaseService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(15);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final String PRODUCT_ID = "BTC-USD";

    private final CoinbaseProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PriceTickAggregators tickAggregators;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    private volatile long lastMessageAtMillis = -1;
    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private volatile WebSocketClient client;

    public CoinbaseService(CoinbaseProperties properties,
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
    }

    public void connect() {
        client = new StandardWebSocketClient();
        String url = properties.wssUrl();

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                lastMessageAtMillis = System.currentTimeMillis();
                log.info("Connected to Coinbase ticker stream for {}", PRODUCT_ID);

                try {
                    // Coinbase disconnects clients that don't subscribe
                    // within 5 seconds of connecting.
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "type", "subscribe",
                            "product_ids", List.of(PRODUCT_ID),
                            "channels", List.of("ticker")
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                    log.info("Subscribed to Coinbase ticker channel for {}", PRODUCT_ID);
                } catch (Exception e) {
                    log.error("Failed to send Coinbase subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                try {
                    onMessage(objectMapper.readTree(message.getPayload()));
                } catch (Exception e) {
                    log.error("Failed to process Coinbase message: {}", message.getPayload(), e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Coinbase websocket transport error, reconnecting", exception);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Coinbase websocket closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, url);
    }

    private void onMessage(JsonNode node) {
        JsonNode typeNode = node.get("type");
        // Ignore subscription acks, heartbeats, and error messages — only
        // "ticker" carries a price.
        if (typeNode == null || !"ticker".equals(typeNode.stringValue())) {
            return;
        }

        JsonNode priceNode = node.get("price");
        if (priceNode == null) return;

        try {
            // Coinbase sends price as a JSON string, not a number.
            BigDecimal price = new BigDecimal(priceNode.stringValue());
            latestPrice.put(CurrencyPairs.BTCUSD, price);
            prices.setCoinbasePrice(price);
            tickAggregators.getCoinbase().record(price);

            lastMessageAtMillis = System.currentTimeMillis();
        } catch (Exception e) {
            log.warn("Failed to parse Coinbase ticker price '{}'", priceNode, e);
        }
    }

    private void checkStaleness() {
        if (lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No Coinbase ticker messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale Coinbase session", e);
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