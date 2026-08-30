package al.r1.polytrader.services.okx;

import al.r1.polytrader.config.okx.OkxProperties;
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

@Slf4j
@Service
public class OkxService {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(15);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration PING_INTERVAL = Duration.ofSeconds(20); // OKX disconnects after ~30s of silence
    private static final String INST_ID = "BTC-USDT";

    private final OkxProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler liveDataTaskScheduler;
    private final Prices prices;
    private final PriceTickAggregators tickAggregators;

    @Getter
    private final Map<CurrencyPairs, BigDecimal> latestPrice = new ConcurrentHashMap<>();

    private volatile long lastMessageAtMillis = -1;
    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private volatile WebSocketClient client;

    public OkxService(OkxProperties properties,
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
                log.info("Connected to OKX trade stream for {}", INST_ID);

                try {
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "op", "subscribe",
                            "args", List.of(Map.of(
                                    "channel", "trades",
                                    "instId", INST_ID
                            ))
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                    log.info("Subscribed to OKX trades channel for {}", INST_ID);
                } catch (Exception e) {
                    log.error("Failed to send OKX subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                String payload = message.getPayload();
                if ("pong".equals(payload)) return;
                try {
                    onMessage(objectMapper.readTree(payload));
                } catch (Exception e) {
                    log.error("Failed to process OKX message: {}", payload, e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("OKX websocket transport error, reconnecting", exception);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("OKX websocket closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, url);
    }

    private void onMessage(JsonNode node) {
        JsonNode argNode = node.get("arg");
        if (argNode == null) return; // subscribe ack / event message

        JsonNode channelNode = argNode.get("channel");
        if (channelNode == null || !"trades".equals(channelNode.stringValue())) return;

        JsonNode data = node.get("data");
        if (data == null || !data.isArray() || data.isEmpty()) return;

        JsonNode lastTrade = data.get(data.size() - 1);
        JsonNode priceNode = lastTrade.get("px");
        if (priceNode == null) return;

        try {
            BigDecimal price = new BigDecimal(priceNode.stringValue());
            latestPrice.put(CurrencyPairs.BTCUSD, price);

            prices.setOkxPrice(price);
            tickAggregators.getOkx().record(price);

            lastMessageAtMillis = System.currentTimeMillis();
        } catch (Exception e) {
            log.warn("Failed to parse OKX trade price '{}'", priceNode, e);
        }
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage("ping"));
            } catch (Exception e) {
                log.warn("Failed to send OKX ping", e);
            }
        }
    }

    private void checkStaleness() {
        if (lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No OKX trade messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale OKX session", e);
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