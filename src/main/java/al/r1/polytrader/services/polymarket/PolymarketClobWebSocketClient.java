package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.model.MarketSide;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PolymarketClobWebSocketClient {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(20);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration PING_INTERVAL = Duration.ofSeconds(10);

    private final PolymarketProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;

    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private volatile long lastMessageAtMillis = -1;
    private volatile boolean running = true;
    private volatile boolean watchdogScheduled = false;

    private final AtomicReference<String> upAssetId = new AtomicReference<>();
    private final AtomicReference<String> downAssetId = new AtomicReference<>();
    private volatile PriceListener listener;

    private final Map<String, ConcurrentSkipListMap<BigDecimal, BigDecimal>> bidsByAsset = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentSkipListMap<BigDecimal, BigDecimal>> asksByAsset = new ConcurrentHashMap<>();

    public interface PriceListener {
        void onPriceUpdate(MarketSide side, BigDecimal bestBid, BigDecimal bestAsk);
    }

    public PolymarketClobWebSocketClient(PolymarketProperties properties,
                                         ObjectMapper objectMapper,
                                         TaskScheduler liveDataTaskScheduler) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;
    }

    public synchronized void start() {
        running = true;
        if (!watchdogScheduled) {
            watchdogScheduled = true;
            taskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
            taskScheduler.scheduleAtFixedRate(this::sendPing, PING_INTERVAL);
        }
        if (upAssetId.get() != null && downAssetId.get() != null && currentSession.get() == null) {
            connect();
        }
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        WebSocketSession session = currentSession.getAndSet(null);
        if (session != null && session.isOpen()) {
            try { session.close(CloseStatus.NORMAL); } catch (Exception ignored) { }
        }
    }

    public synchronized void subscribe(String newUpAssetId, String newDownAssetId, PriceListener newListener) {
        if (newUpAssetId == null || newDownAssetId == null) return;

        this.listener = newListener;

        boolean changed = !newUpAssetId.equals(upAssetId.get()) || !newDownAssetId.equals(downAssetId.get());
        if (!changed && currentSession.get() != null) {
            return;
        }

        upAssetId.set(newUpAssetId);
        downAssetId.set(newDownAssetId);
        bidsByAsset.clear();
        asksByAsset.clear();

        log.info("CLOB resubscribing: up={} down={}", newUpAssetId, newDownAssetId);

        WebSocketSession oldSession = currentSession.getAndSet(null);
        if (oldSession != null && oldSession.isOpen()) {
            try { oldSession.close(CloseStatus.NORMAL); } catch (Exception ignored) { }
        }
        connect();
    }

    private synchronized void connect() {
        if (!running) return;

        String up = upAssetId.get();
        String down = downAssetId.get();
        if (up == null || down == null) return;

        String url = properties.clobWssUrl();
        if (url == null || url.isBlank()) {
            log.warn("services.polymarket.clob-wss-url is not set; cannot start CLOB price stream");
            return;
        }

        WebSocketClient client = new StandardWebSocketClient();

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                lastMessageAtMillis = System.currentTimeMillis();
                log.info("Connected to Polymarket CLOB market stream, subscribing up={} down={}", up, down);

                try {
                    String subscribeMsg = objectMapper.writeValueAsString(Map.of(
                            "type", "market",
                            "assets_ids", List.of(up, down)
                    ));
                    session.sendMessage(new TextMessage(subscribeMsg));
                } catch (Exception e) {
                    log.error("Failed to send CLOB subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                String payload = message.getPayload();
                lastMessageAtMillis = System.currentTimeMillis();
                if ("PONG".equals(payload)) return;
                try {
                    onMessage(objectMapper.readTree(payload));
                } catch (Exception e) {
                    log.error("Failed to process CLOB message: {}", payload, e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("CLOB websocket transport error, reconnecting", exception);
                if (currentSession.compareAndSet(session, null)) {
                    scheduleReconnect();
                }
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("CLOB websocket closed ({}), reconnecting", closeStatus);
                if (currentSession.compareAndSet(session, null)) {
                    scheduleReconnect();
                }
            }
        }, url);
    }

    private void onMessage(JsonNode node) {
        if (node.isArray()) {
            node.forEach(this::onEvent);
        } else {
            onEvent(node);
        }
    }

    private void onEvent(JsonNode event) {
        JsonNode typeNode = event.get("event_type");
        JsonNode assetIdNode = event.get("asset_id");
        if (typeNode == null || assetIdNode == null) return;

        String assetId = assetIdNode.stringValue();
        MarketSide side = sideForAsset(assetId);
        if (side == null) return; // stale event for an asset we've since moved on from

        String type = typeNode.stringValue();
        if ("book".equals(type)) {
            applyFullBook(assetId, event);
        } else if ("price_change".equals(type)) {
            applyPriceChanges(assetId, event);
        } else {
            return; // e.g. tick_size_change, last_trade_price — not needed for pricing
        }

        publishBest(side, assetId);
    }

    private MarketSide sideForAsset(String assetId) {
        if (assetId.equals(upAssetId.get())) return MarketSide.UP;
        if (assetId.equals(downAssetId.get())) return MarketSide.DOWN;
        return null;
    }

    private void applyFullBook(String assetId, JsonNode event) {
        ConcurrentSkipListMap<BigDecimal, BigDecimal> bids = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        ConcurrentSkipListMap<BigDecimal, BigDecimal> asks = new ConcurrentSkipListMap<>();
        populateLevels(bids, event.get("bids"));
        populateLevels(asks, event.get("asks"));
        bidsByAsset.put(assetId, bids);
        asksByAsset.put(assetId, asks);
    }

    private void populateLevels(Map<BigDecimal, BigDecimal> target, JsonNode levelsNode) {
        if (levelsNode == null || !levelsNode.isArray()) return;
        levelsNode.forEach(level -> {
            BigDecimal price = parseDecimal(level.get("price"));
            BigDecimal size = parseDecimal(level.get("size"));
            if (price == null || size == null || size.signum() <= 0) return;
            target.put(price, size);
        });
    }

    private void applyPriceChanges(String assetId, JsonNode event) {
        JsonNode changes = event.get("changes");
        if (changes == null || !changes.isArray()) return;

        ConcurrentSkipListMap<BigDecimal, BigDecimal> bids =
                bidsByAsset.computeIfAbsent(assetId, k -> new ConcurrentSkipListMap<>(Comparator.reverseOrder()));
        ConcurrentSkipListMap<BigDecimal, BigDecimal> asks =
                asksByAsset.computeIfAbsent(assetId, k -> new ConcurrentSkipListMap<>());

        changes.forEach(change -> {
            BigDecimal price = parseDecimal(change.get("price"));
            JsonNode sideNode = change.get("side");
            if (price == null || sideNode == null) return;

            BigDecimal size = parseDecimal(change.get("size"));
            Map<BigDecimal, BigDecimal> book = "BUY".equalsIgnoreCase(sideNode.stringValue()) ? bids : asks;

            if (size == null || size.signum() <= 0) {
                book.remove(price);
            } else {
                book.put(price, size);
            }
        });
    }

    private void publishBest(MarketSide side, String assetId) {
        ConcurrentSkipListMap<BigDecimal, BigDecimal> bids = bidsByAsset.get(assetId);
        ConcurrentSkipListMap<BigDecimal, BigDecimal> asks = asksByAsset.get(assetId);
        if (bids == null || asks == null || bids.isEmpty() || asks.isEmpty()) return;

        BigDecimal bestBid = bids.firstKey();
        BigDecimal bestAsk = asks.firstKey();

        PriceListener currentListener = listener;
        if (currentListener != null) {
            currentListener.onPriceUpdate(side, bestBid, bestAsk);
        }
    }

    private BigDecimal parseDecimal(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            return node.isNumber() ? node.decimalValue() : new BigDecimal(node.stringValue());
        } catch (Exception e) {
            return null;
        }
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage("PING"));
            } catch (Exception e) {
                log.warn("Failed to send CLOB ping", e);
            }
        }
    }

    private void checkStaleness() {
        if (!running || lastMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("No CLOB messages in {}ms, forcing reconnect", silentFor);

            WebSocketSession session = currentSession.getAndSet(null);
            if (session != null && session.isOpen()) {
                try {
                    session.close(CloseStatus.GOING_AWAY);
                } catch (Exception e) {
                    log.warn("Failed to close stale CLOB session", e);
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