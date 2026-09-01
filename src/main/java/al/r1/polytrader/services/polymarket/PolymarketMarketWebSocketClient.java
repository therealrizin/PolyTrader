package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.model.MarketSide;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PolymarketMarketWebSocketClient {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(20);
    private static final Duration STALENESS_CHECK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration PING_INTERVAL = Duration.ofSeconds(10);

    private final PolymarketProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;
    private final HttpClient httpClient;

    private final AtomicReference<WebSocketClient> currentClient = new AtomicReference<>();
    private final AtomicReference<String> currentSlug = new AtomicReference<>();
    private final AtomicLong connectionGeneration = new AtomicLong(0);
    private final AtomicBoolean reconnectPending = new AtomicBoolean(false);

    private volatile String upTokenId;
    private volatile String downTokenId;
    private volatile long lastMarketDataMessageAtMillis = -1;
    private volatile boolean running = false;
    private volatile PriceListener listener;

    private final Map<MarketSide, BigDecimal> bestBidBySide = new ConcurrentHashMap<>();
    private final Map<MarketSide, BigDecimal> bestAskBySide = new ConcurrentHashMap<>();

    public interface PriceListener {
        void onPriceUpdate(MarketSide side, BigDecimal bestBid, BigDecimal bestAsk);
    }

    public PolymarketMarketWebSocketClient(PolymarketProperties properties,
                                           ObjectMapper objectMapper,
                                           TaskScheduler liveDataTaskScheduler) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @PostConstruct
    public void init() {
        start();
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        scheduleWatchdog();
        scheduleHeartbeat();
        log.info("Polymarket market WebSocket client started");
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        connectionGeneration.incrementAndGet();
        closeClient();
        log.info("Polymarket market WebSocket client stopped");
    }

    public synchronized void subscribe(String slug, PriceListener newListener) {
        if (slug == null || slug.isBlank()) {
            log.warn("Cannot subscribe to blank Polymarket slug");
            return;
        }
        if (!running) start();

        this.listener = newListener;
        String previousSlug = currentSlug.get();
        WebSocketClient existingClient = currentClient.get();

        if (slug.equals(previousSlug) && upTokenId != null && downTokenId != null
                && existingClient != null && existingClient.isOpen()) {
            log.debug("Already subscribed to Polymarket market: {}", slug);
            return;
        }

        log.info("Resolving Polymarket market: slug={}", slug);
        try {
            TokenIds tokenIds = getTokenIds(slug);
            long generation = connectionGeneration.incrementAndGet();
            this.upTokenId = tokenIds.upTokenId();
            this.downTokenId = tokenIds.downTokenId();
            currentSlug.set(slug);
            bestBidBySide.clear();
            bestAskBySide.clear();
            lastMarketDataMessageAtMillis = System.currentTimeMillis();

            log.info("Resolved Polymarket market: slug={}, UP={}, DOWN={}, generation={}",
                    slug, upTokenId, downTokenId, generation);
            closeClient();
            connect(generation);
        } catch (Exception e) {
            log.error("Failed to resolve Polymarket market: slug={}", slug, e);
            scheduleReconnect();
        }
    }

    // ------------------------------------------------------------------------
    // Gamma API
    // ------------------------------------------------------------------------
    private TokenIds getTokenIds(String slug) throws Exception {
        String baseUrl = properties.gammaBaseUrl();
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException("services.polymarket.gamma-base-url is not configured");
        }
        String url = baseUrl.replaceAll("/+$", "") + "/markets/slug/" + URI.create("/" + slug).getPath().substring(1);
        log.debug("Fetching Polymarket market metadata: {}", url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Gamma API returned HTTP " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());
        JsonNode clobTokenIdsNode = root.get("clobTokenIds");
        if (clobTokenIdsNode == null || clobTokenIdsNode.isNull()) {
            throw new IllegalStateException("Market has no clobTokenIds: " + slug);
        }

        List<String> tokenIds;
        if (clobTokenIdsNode.isTextual()) {
            JsonNode parsed = objectMapper.readTree(clobTokenIdsNode.asText());
            if (!parsed.isArray()) {
                throw new IllegalStateException("clobTokenIds is not an array: " + clobTokenIdsNode);
            }
            tokenIds = parseTokenArray(parsed);
        } else if (clobTokenIdsNode.isArray()) {
            tokenIds = parseTokenArray(clobTokenIdsNode);
        } else {
            throw new IllegalStateException("Unexpected clobTokenIds format: " + clobTokenIdsNode);
        }

        if (tokenIds.size() < 2) {
            throw new IllegalStateException("Expected YES and NO token IDs for market " + slug + ", got: " + tokenIds);
        }
        return new TokenIds(tokenIds.get(0), tokenIds.get(1));
    }

    private List<String> parseTokenArray(JsonNode node) {
        List<String> result = new ArrayList<>();
        for (JsonNode item : node) {
            if (item != null && !item.isNull() && !item.asText().isBlank()) {
                result.add(item.asText());
            }
        }
        return result;
    }

    // ------------------------------------------------------------------------
    // WebSocket connection
    // ------------------------------------------------------------------------
    private synchronized void connect(long generation) {
        if (!running || generation != connectionGeneration.get()) {
            log.debug("Ignoring obsolete connect request: generation={}, current={}",
                    generation, connectionGeneration.get());
            return;
        }
        String slug = currentSlug.get();
        if (slug == null || upTokenId == null || downTokenId == null) {
            log.warn("No market or token IDs available, will retry");
            scheduleReconnect();
            return;
        }

        String url = properties.clobWssUrl();
        if (url == null || url.isBlank()) {
            log.error("services.polymarket.clob-wss-url is not configured");
            return;
        }

        WebSocketClient existing = currentClient.get();
        if (existing != null && existing.isOpen()) return;

        final String connectionSlug = slug;
        final String connectionUpToken = upTokenId;
        final String connectionDownToken = downTokenId;
        final long connectionId = generation;

        try {
            URI serverUri = URI.create(url);
            WebSocketClient client = new WebSocketClient(serverUri, new Draft_6455()) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    if (!isCurrentConnection(this, connectionId)) {
                        log.debug("Ignoring onOpen from obsolete connection: generation={}", connectionId);
                        try { close(); } catch (Exception ignored) {}
                        return;
                    }
                    log.info("Connected to Polymarket CLOB WebSocket: slug={}, generation={}", connectionSlug, connectionId);
                    lastMarketDataMessageAtMillis = System.currentTimeMillis();
                    sendSubscription(this, connectionUpToken, connectionDownToken, connectionId);
                }

                @Override
                public void onMessage(String message) {
                    if (!isCurrentConnection(this, connectionId)) {
                        log.debug("Ignoring message from obsolete connection: generation={}", connectionId);
                        return;
                    }
                    if ("PONG".equalsIgnoreCase(message.trim())) {
                        log.trace("Received Polymarket PONG");
                        return;
                    }
                    lastMarketDataMessageAtMillis = System.currentTimeMillis();
                    log.debug("Polymarket WS message: {}", message.length() > 2000 ? message.substring(0, 2000) + "..." : message);

                    try {
                        JsonNode root = objectMapper.readTree(message);
                        if (root.isArray()) {
                            for (JsonNode event : root) {
                                if (event.isObject()) {
                                    handleMessage(event, connectionId, connectionUpToken, connectionDownToken);
                                }
                            }
                        } else if (root.isObject()) {
                            handleMessage(root, connectionId, connectionUpToken, connectionDownToken);
                        } else {
                            log.debug("Ignoring unexpected Polymarket WS message type: {}", root.getNodeType());
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse Polymarket WS message: {}", message, e);
                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.warn("Polymarket WebSocket closed: code={}, reason={}, remote={}, generation={}",
                            code, reason, remote, connectionId);
                    if (!isCurrentConnection(this, connectionId)) {
                        log.debug("Ignoring onClose from obsolete connection: generation={}", connectionId);
                        return;
                    }
                    currentClient.compareAndSet(this, null);
                    if (running) scheduleReconnect();
                }

                @Override
                public void onError(Exception ex) {
                    if (!isCurrentConnection(this, connectionId)) return;
                    log.error("Polymarket WebSocket error: generation={}", connectionId, ex);
                }
            };

            currentClient.set(client);
            log.info("Connecting to Polymarket CLOB WebSocket: slug={}, generation={}, url={}",
                    connectionSlug, connectionId, serverUri);
            client.connect();
        } catch (Exception e) {
            log.error("Failed to create Polymarket WebSocket client", e);
            if (generation == connectionGeneration.get()) {
                currentClient.set(null);
                scheduleReconnect();
            }
        }
    }

    private boolean isCurrentConnection(WebSocketClient client, long generation) {
        return running && generation == connectionGeneration.get() && currentClient.get() == client;
    }

    // ------------------------------------------------------------------------
    // Subscription
    // ------------------------------------------------------------------------
    private void sendSubscription(WebSocketClient client, String upToken, String downToken, long generation) {
        if (upToken == null || downToken == null) {
            log.error("Cannot subscribe: token IDs are missing");
            return;
        }
        if (!isCurrentConnection(client, generation)) return;

        try {
            String subscribeMessage = objectMapper.writeValueAsString(Map.of(
                    "assets_ids", List.of(upToken, downToken),
                    "type", "market",
                    "custom_feature_enabled", true
            ));
            if (!client.isOpen()) {
                log.warn("Cannot subscribe: WebSocket is not open");
                return;
            }
            client.send(subscribeMessage);
            log.info("Polymarket subscription sent: slug={}, generation={}, UP={}, DOWN={}",
                    currentSlug.get(), generation, upToken, downToken);
        } catch (Exception e) {
            log.error("Failed to send Polymarket subscription", e);
        }
    }

    // ------------------------------------------------------------------------
    // Message handling
    // ------------------------------------------------------------------------
    private void handleMessage(JsonNode root, long generation, String connectionUpToken, String connectionDownToken) {
        String eventType = root.path("event_type").asText("");
        if (eventType.isBlank()) {
            log.debug("Polymarket message without event_type: {}", root);
            return;
        }
        switch (eventType) {
            case "book" -> handleBook(root, generation, connectionUpToken, connectionDownToken);
            case "price_change" -> handlePriceChange(root, generation, connectionUpToken, connectionDownToken);
            case "best_bid_ask" -> handleBestBidAsk(root, generation, connectionUpToken, connectionDownToken);
            case "last_trade_price" -> handleLastTradePrice(root, connectionUpToken, connectionDownToken);
            case "tick_size_change" -> log.debug("Polymarket tick size changed: {}", root);
            case "new_market" -> log.debug("Polymarket new market event: {}", root);
            case "market_resolved" -> log.info("Polymarket market resolved: {}", root);
            default -> log.debug("Unhandled Polymarket event_type={}: {}", eventType, root);
        }
    }

    // ------------------------------------------------------------------------
    // Specific handlers
    // ------------------------------------------------------------------------
    private void handleBook(JsonNode root, long generation, String connectionUpToken, String connectionDownToken) {
        String assetId = root.path("asset_id").asText(null);
        MarketSide side = mapTokenToSide(assetId, connectionUpToken, connectionDownToken);
        if (side == null) {
            log.warn("Received book for unknown asset: {}", assetId);
            return;
        }
        BigDecimal bid = findBestBid(root.path("bids"));
        BigDecimal ask = findBestAsk(root.path("asks"));
        updateBestPrices(side, bid, ask);
        log.debug("POLYMARKET BOOK: slug={} side={} bid={} ask={}", currentSlug.get(), side, bid, ask);
    }

    private void handleBestBidAsk(JsonNode root, long generation, String connectionUpToken, String connectionDownToken) {
        String assetId = root.path("asset_id").asText(null);
        MarketSide side = mapTokenToSide(assetId, connectionUpToken, connectionDownToken);
        if (side == null) {
            log.warn("Received best_bid_ask for unknown asset: {}", assetId);
            return;
        }
        BigDecimal bid = parseDecimal(root.get("best_bid"));
        BigDecimal ask = parseDecimal(root.get("best_ask"));
        updateBestPrices(side, bid, ask);
        log.debug("POLYMARKET BBO: slug={} side={} bid={} ask={} spread={}",
                currentSlug.get(), side, bid, ask, parseDecimal(root.get("spread")));
    }

    private void handlePriceChange(JsonNode root, long generation, String connectionUpToken, String connectionDownToken) {
        JsonNode changes = root.path("price_changes");
        if (!changes.isArray()) {
            log.debug("price_change without price_changes array: {}", root);
            return;
        }
        for (JsonNode change : changes) {
            String assetId = change.path("asset_id").asText(null);
            MarketSide side = mapTokenToSide(assetId, connectionUpToken, connectionDownToken);
            if (side == null) {
                log.warn("Received price_change for unknown asset: {}", assetId);
                continue;
            }
            BigDecimal bid = parseDecimal(change.get("best_bid"));
            BigDecimal ask = parseDecimal(change.get("best_ask"));
            BigDecimal price = parseDecimal(change.get("price"));
            updateBestPrices(side, bid, ask);
            log.debug("POLYMARKET PRICE: slug={} side={} asset={} price={} bid={} ask={}",
                    currentSlug.get(), side, assetId, price, bid, ask);
        }
    }

    private void handleLastTradePrice(JsonNode root, String connectionUpToken, String connectionDownToken) {
        String assetId = root.path("asset_id").asText(null);
        MarketSide side = mapTokenToSide(assetId, connectionUpToken, connectionDownToken);
        if (side == null) return;
        BigDecimal price = parseDecimal(root.get("price"));
        if (price != null) {
            log.trace("Polymarket last trade: side={}, price={}", side, price);
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------
    private MarketSide mapTokenToSide(String assetId, String upToken, String downToken) {
        if (assetId == null) return null;
        if (assetId.equals(upToken)) return MarketSide.UP;
        if (assetId.equals(downToken)) return MarketSide.DOWN;
        return null;
    }

    private BigDecimal findBestBid(JsonNode bids) {
        if (!bids.isArray()) return null;
        BigDecimal best = null;
        for (JsonNode level : bids) {
            BigDecimal price = parseDecimal(level.get("price"));
            if (price != null && (best == null || price.compareTo(best) > 0)) {
                best = price;
            }
        }
        return best;
    }

    private BigDecimal findBestAsk(JsonNode asks) {
        if (!asks.isArray()) return null;
        BigDecimal best = null;
        for (JsonNode level : asks) {
            BigDecimal price = parseDecimal(level.get("price"));
            if (price != null && (best == null || price.compareTo(best) < 0)) {
                best = price;
            }
        }
        return best;
    }

    private BigDecimal parseDecimal(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            if (node.isNumber()) return node.decimalValue();
            String val = node.asText();
            return (val == null || val.isBlank()) ? null : new BigDecimal(val);
        } catch (Exception e) {
            return null;
        }
    }

    // Centralised update – handles nulls correctly
    private void updateBestPrices(MarketSide side, BigDecimal bid, BigDecimal ask) {
        if (bid != null) bestBidBySide.put(side, bid);
        else bestBidBySide.remove(side);

        if (ask != null) bestAskBySide.put(side, ask);
        else bestAskBySide.remove(side);

        publish(side);
    }

    private void publish(MarketSide side) {
        BigDecimal bid = bestBidBySide.get(side);
        BigDecimal ask = bestAskBySide.get(side);
        if (bid == null || ask == null) return;
        PriceListener current = listener;
        if (current != null) {
            try {
                current.onPriceUpdate(side, bid, ask);
            } catch (Exception e) {
                log.error("Polymarket PriceListener failed", e);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Heartbeat & Watchdog
    // ------------------------------------------------------------------------
    private synchronized void scheduleHeartbeat() {
        taskScheduler.scheduleAtFixedRate(this::sendPing, PING_INTERVAL);
    }

    private void sendPing() {
        if (!running) return;
        WebSocketClient client = currentClient.get();
        if (client != null && client.isOpen()) {
            try {
                client.send("PING");
                log.trace("Sent Polymarket PING");
            } catch (Exception e) {
                log.warn("Failed to send Polymarket PING", e);
            }
        }
    }

    private synchronized void scheduleWatchdog() {
        taskScheduler.scheduleAtFixedRate(this::checkStaleness, STALENESS_CHECK_INTERVAL);
    }

    private void checkStaleness() {
        if (!running) return;
        WebSocketClient client = currentClient.get();
        if (client == null || !client.isOpen()) return;
        if (lastMarketDataMessageAtMillis < 0) return;

        long silentFor = System.currentTimeMillis() - lastMarketDataMessageAtMillis;
        if (silentFor > STALE_THRESHOLD.toMillis()) {
            log.warn("Polymarket market data stale for {}ms. Reconnecting.", silentFor);
            connectionGeneration.incrementAndGet();
            closeClient();
            lastMarketDataMessageAtMillis = System.currentTimeMillis();
            scheduleReconnect();
        }
    }

    // ------------------------------------------------------------------------
    // Reconnection
    // ------------------------------------------------------------------------
    private synchronized void scheduleReconnect() {
        if (!running) return;
        if (reconnectPending.getAndSet(true)) return; // already scheduled

        WebSocketClient client = currentClient.get();
        if (client != null && client.isOpen()) {
            reconnectPending.set(false);
            return;
        }

        taskScheduler.schedule(() -> {
            reconnectPending.set(false);
            reconnect();
        }, Instant.now().plus(RECONNECT_DELAY));
    }

    private void reconnect() {
        if (!running) return;
        String slug = currentSlug.get();
        if (slug == null) return;

        WebSocketClient client = currentClient.get();
        if (client != null && client.isOpen()) return;

        long generation = connectionGeneration.incrementAndGet();
        log.info("Reconnecting to Polymarket market: slug={}, generation={}", slug, generation);
        connect(generation);
    }

    // ------------------------------------------------------------------------
    // Close
    // ------------------------------------------------------------------------
    private void closeClient() {
        WebSocketClient client = currentClient.getAndSet(null);
        if (client != null) {
            try {
                if (!client.isClosed()) client.close(1000, "Normal closure");
            } catch (Exception e) {
                log.debug("Error closing Polymarket WebSocket", e);
            }
        }
    }

    private record TokenIds(String upTokenId, String downTokenId) {}
}