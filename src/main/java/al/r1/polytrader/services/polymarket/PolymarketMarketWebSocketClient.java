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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PolymarketMarketWebSocketClient implements PolymarketMarketResolver {

    private static final Duration RECONNECT_DELAY =
            Duration.ofSeconds(3);

    private static final Duration STALE_THRESHOLD =
            Duration.ofSeconds(20);

    private static final Duration STALENESS_CHECK_INTERVAL =
            Duration.ofSeconds(5);

    private static final Duration PING_INTERVAL =
            Duration.ofSeconds(10);

    private final PolymarketProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;
    private final HttpClient httpClient;

    private final AtomicReference<WebSocketClient> currentClient =
            new AtomicReference<>();

    private final AtomicReference<String> currentSlug =
            new AtomicReference<>();

    private final AtomicLong connectionGeneration =
            new AtomicLong();

    private final AtomicBoolean reconnectPending =
            new AtomicBoolean(false);

    private volatile String upTokenId;
    private volatile String downTokenId;

    private volatile long lastMarketDataMessageAtMillis = -1;

    private volatile boolean running;

    private volatile PriceListener listener;

    private final Map<MarketSide, BigDecimal> bestBidBySide =
            new ConcurrentHashMap<>();

    private final Map<MarketSide, BigDecimal> bestAskBySide =
            new ConcurrentHashMap<>();

    public interface PriceListener {

        void onPriceUpdate(
                MarketSide side,
                BigDecimal bestBid,
                BigDecimal bestAsk
        );
    }

    @Override
    public Optional<ResolvedMarket> resolveCurrentMarket() {
        String slug = currentSlug.get();
        String up = upTokenId;
        String down = downTokenId;

        if (slug == null || up == null || down == null) {
            return Optional.empty();
        }

        return Optional.of(new ResolvedMarket(slug, up, down));
    }

    public PolymarketMarketWebSocketClient(
            PolymarketProperties properties,
            ObjectMapper objectMapper,
            TaskScheduler liveDataTaskScheduler) {

        this.properties = properties;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;

        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
    }

    @PostConstruct
    public void init() {
        start();
    }

    public synchronized void start() {

        if (running) {
            return;
        }

        running = true;

        taskScheduler.scheduleAtFixedRate(
                this::sendPing,
                PING_INTERVAL
        );

        taskScheduler.scheduleAtFixedRate(
                this::checkStaleness,
                STALENESS_CHECK_INTERVAL
        );

        log.info(
                "Polymarket market WebSocket client started"
        );
    }

    @PreDestroy
    public synchronized void stop() {

        running = false;

        connectionGeneration.incrementAndGet();

        closeClient();

        log.info(
                "Polymarket market WebSocket client stopped"
        );
    }

    /**
     * Subscribe to a market. If the same slug is already subscribed, this method
     * will NOT re‑resolve token IDs or tear down the connection – it simply
     * updates the listener and (if the connection is broken) schedules a reconnect.
     */
    public synchronized void subscribe(
            String slug,
            PriceListener newListener) {

        if (slug == null || slug.isBlank()) {
            return;
        }

        if (!running) {
            start();
        }

        // Always update the listener – it may be a new one.
        listener = newListener;

        String previousSlug = currentSlug.get();
        WebSocketClient existingClient = currentClient.get();

        // Same market and we already have token IDs: no need to re‑resolve or reconnect immediately.
        if (slug.equals(previousSlug) && upTokenId != null && downTokenId != null) {
            // If the connection is not open, schedule a reconnect (if not already pending)
            if (existingClient == null || !existingClient.isOpen()) {
                if (!reconnectPending.get()) {
                    scheduleReconnect();
                }
            }
            return;
        }

        // Different market (or missing token IDs) – we must resolve and connect.
        log.info("POLYMARKET WS subscribing to market slug={}", slug);

        try {
            TokenIds tokenIds = getTokenIds(slug);
            long generation = connectionGeneration.incrementAndGet();

            upTokenId = tokenIds.upTokenId();
            downTokenId = tokenIds.downTokenId();
            currentSlug.set(slug);

            // Clear old prices – they belong to a different market.
            bestBidBySide.clear();
            bestAskBySide.clear();

            lastMarketDataMessageAtMillis = System.currentTimeMillis();

            // Close any existing client before creating a new one.
            closeClient();

            log.info("POLYMARKET WS market resolved: slug={} UP={} DOWN={} generation={}",
                    slug, upTokenId, downTokenId, generation);

            connect(generation);

        } catch (Exception e) {
            log.error("Failed to resolve Polymarket market: slug={}", slug, e);
            scheduleReconnect();
        }
    }

    private TokenIds getTokenIds(
            String slug) throws Exception {

        String baseUrl =
                properties.gammaBaseUrl();

        if (baseUrl == null
                || baseUrl.isBlank()) {

            throw new IllegalStateException(
                    "services.polymarket.gamma-base-url " +
                            "is not configured"
            );
        }

        String url =
                baseUrl.replaceAll("/+$", "")
                        + "/markets/slug/"
                        + slug;

        log.debug(
                "Fetching Polymarket CLOB token IDs: {}",
                url
        );

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header(
                                "Accept",
                                "application/json"
                        )
                        .GET()
                        .build();

        HttpResponse<String> response =
                httpClient.send(
                        request,
                        HttpResponse.BodyHandlers.ofString()
                );

        if (response.statusCode() != 200) {

            throw new IllegalStateException(
                    "Gamma API returned HTTP "
                            + response.statusCode()
                            + ": "
                            + response.body()
            );
        }

        JsonNode root =
                objectMapper.readTree(
                        response.body()
                );

        JsonNode tokenNode =
                root.get("clobTokenIds");

        if (tokenNode == null
                || tokenNode.isNull()) {

            throw new IllegalStateException(
                    "Market has no clobTokenIds: "
                            + slug
            );
        }

        List<String> tokenIds;

        if (tokenNode.isTextual()) {

            JsonNode parsed =
                    objectMapper.readTree(
                            tokenNode.asText()
                    );

            if (!parsed.isArray()) {

                throw new IllegalStateException(
                        "clobTokenIds is not an array: "
                                + tokenNode
                );
            }

            tokenIds =
                    parseTokenArray(parsed);

        } else if (tokenNode.isArray()) {

            tokenIds =
                    parseTokenArray(tokenNode);

        } else {

            throw new IllegalStateException(
                    "Unexpected clobTokenIds format: "
                            + tokenNode
            );
        }

        if (tokenIds.size() < 2) {

            throw new IllegalStateException(
                    "Expected UP and DOWN token IDs for "
                            + slug
                            + ", got "
                            + tokenIds
            );
        }

        return new TokenIds(
                tokenIds.get(0),
                tokenIds.get(1)
        );
    }

    private List<String> parseTokenArray(
            JsonNode node) {

        List<String> result =
                new ArrayList<>();

        for (JsonNode item : node) {

            if (item != null
                    && !item.isNull()
                    && !item.asText().isBlank()) {

                result.add(
                        item.asText()
                );
            }
        }

        return result;
    }

    private synchronized void connect(
            long generation) {

        if (!running
                || generation != connectionGeneration.get()) {

            return;
        }

        String slug =
                currentSlug.get();

        if (slug == null
                || upTokenId == null
                || downTokenId == null) {

            scheduleReconnect();
            return;
        }

        String url =
                properties.clobWssUrl();

        if (url == null
                || url.isBlank()) {

            log.error(
                    "services.polymarket.clob-wss-url " +
                            "is not configured"
            );

            return;
        }

        WebSocketClient existing =
                currentClient.get();

        if (existing != null
                && existing.isOpen()) {

            return;
        }

        final String connectionSlug =
                slug;

        final String connectionUpToken =
                upTokenId;

        final String connectionDownToken =
                downTokenId;

        final long connectionId =
                generation;

        try {

            URI serverUri =
                    URI.create(url);

            WebSocketClient client =
                    new WebSocketClient(
                            serverUri,
                            new Draft_6455()
                    ) {

                        @Override
                        public void onOpen(
                                ServerHandshake handshake) {

                            if (!isCurrentConnection(
                                    this,
                                    connectionId)) {

                                close();
                                return;
                            }

                            log.info(
                                    "POLYMARKET WS CONNECTED " +
                                            "slug={} generation={}",
                                    connectionSlug,
                                    connectionId
                            );

                            lastMarketDataMessageAtMillis =
                                    System.currentTimeMillis();

                            sendSubscription(
                                    this,
                                    connectionUpToken,
                                    connectionDownToken,
                                    connectionId
                            );
                        }

                        @Override
                        public void onMessage(
                                String message) {

                            if (!isCurrentConnection(
                                    this,
                                    connectionId)) {

                                return;
                            }

                            if ("PONG".equalsIgnoreCase(
                                    message.trim())) {

                                return;
                            }

                            lastMarketDataMessageAtMillis =
                                    System.currentTimeMillis();

                            try {

                                JsonNode root =
                                        objectMapper.readTree(
                                                message
                                        );

                                /*
                                 * Polymarket can send either:
                                 *
                                 * { ... }
                                 *
                                 * or
                                 *
                                 * [{...}, {...}]
                                 *
                                 * Handle BOTH.
                                 */
                                if (root.isArray()) {

                                    for (JsonNode event : root) {

                                        if (event.isObject()) {

                                            handleMessage(
                                                    event,
                                                    connectionId,
                                                    connectionUpToken,
                                                    connectionDownToken
                                            );
                                        }
                                    }

                                } else if (root.isObject()) {

                                    handleMessage(
                                            root,
                                            connectionId,
                                            connectionUpToken,
                                            connectionDownToken
                                    );
                                }

                            } catch (Exception e) {

                                log.error(
                                        "Failed to parse Polymarket WS message: {}",
                                        message,
                                        e
                                );
                            }
                        }

                        @Override
                        public void onClose(
                                int code,
                                String reason,
                                boolean remote) {

                            log.warn(
                                    "POLYMARKET WS CLOSED " +
                                            "code={} reason={} remote={} generation={}",
                                    code,
                                    reason,
                                    remote,
                                    connectionId
                            );

                            if (!isCurrentConnection(
                                    this,
                                    connectionId)) {

                                return;
                            }

                            currentClient.compareAndSet(
                                    this,
                                    null
                            );

                            if (running) {
                                scheduleReconnect();
                            }
                        }

                        @Override
                        public void onError(
                                Exception ex) {

                            if (!isCurrentConnection(
                                    this,
                                    connectionId)) {

                                return;
                            }

                            log.error(
                                    "POLYMARKET WS ERROR generation={}",
                                    connectionId,
                                    ex
                            );
                        }
                    };

            currentClient.set(client);

            log.info(
                    "Connecting to Polymarket CLOB WS: " +
                            "slug={} generation={} url={}",
                    connectionSlug,
                    connectionId,
                    serverUri
            );

            client.connect();

        } catch (Exception e) {

            log.error(
                    "Failed to create Polymarket WebSocket client",
                    e
            );

            if (generation ==
                    connectionGeneration.get()) {

                currentClient.set(null);

                scheduleReconnect();
            }
        }
    }

    private boolean isCurrentConnection(
            WebSocketClient client,
            long generation) {

        return running
                && generation == connectionGeneration.get()
                && currentClient.get() == client;
    }

    private void sendSubscription(
            WebSocketClient client,
            String upToken,
            String downToken,
            long generation) {

        if (!isCurrentConnection(
                client,
                generation)) {

            return;
        }

        try {

            String message =
                    objectMapper.writeValueAsString(
                            Map.of(
                                    "assets_ids",
                                    List.of(
                                            upToken,
                                            downToken
                                    ),
                                    "type",
                                    "market",
                                    "custom_feature_enabled",
                                    true
                            )
                    );

            if (!client.isOpen()) {
                return;
            }

            client.send(message);

            log.info(
                    "POLYMARKET WS SUBSCRIBED " +
                            "slug={} UP={} DOWN={}",
                    currentSlug.get(),
                    upToken,
                    downToken
            );

        } catch (Exception e) {

            log.error(
                    "Failed to send Polymarket subscription",
                    e
            );
        }
    }

    private void handleMessage(
            JsonNode root,
            long generation,
            String upToken,
            String downToken) {

        String eventType =
                root.path("event_type")
                        .asText("");

        if (eventType.isBlank()) {
            return;
        }

        switch (eventType) {

            case "book" ->
                    handleBook(
                            root,
                            upToken,
                            downToken
                    );

            case "price_change" ->
                    handlePriceChange(
                            root,
                            upToken,
                            downToken
                    );

            case "best_bid_ask" ->
                    handleBestBidAsk(
                            root,
                            upToken,
                            downToken
                    );

            case "last_trade_price" ->
                    handleLastTradePrice(
                            root,
                            upToken,
                            downToken
                    );

            case "tick_size_change" ->
                    log.debug(
                            "Polymarket tick size change: {}",
                            root
                    );

            case "market_resolved" ->
                    log.info(
                            "Polymarket market resolved: {}",
                            root
                    );

            default ->
                    log.debug(
                            "Unhandled Polymarket event_type={}",
                            eventType
                    );
        }
    }

    private void handleBook(
            JsonNode root,
            String upToken,
            String downToken) {

        String assetId =
                root.path("asset_id")
                        .asText(null);

        MarketSide side =
                mapTokenToSide(
                        assetId,
                        upToken,
                        downToken
                );

        if (side == null) {
            return;
        }

        BigDecimal bid =
                findBestBid(
                        root.path("bids")
                );

        BigDecimal ask =
                findBestAsk(
                        root.path("asks")
                );

        updateBestPrices(
                side,
                bid,
                ask
        );

        log.debug(
                "POLYMARKET BOOK slug={} side={} bid={} ask={}",
                currentSlug.get(),
                side,
                bid,
                ask
        );
    }

    private void handleBestBidAsk(
            JsonNode root,
            String upToken,
            String downToken) {

        String assetId =
                root.path("asset_id")
                        .asText(null);

        MarketSide side =
                mapTokenToSide(
                        assetId,
                        upToken,
                        downToken
                );

        if (side == null) {
            return;
        }

        BigDecimal bid =
                parseDecimal(
                        root.get("best_bid")
                );

        BigDecimal ask =
                parseDecimal(
                        root.get("best_ask")
                );

        updateBestPrices(
                side,
                bid,
                ask
        );

        log.debug(
                "POLYMARKET BBO slug={} side={} bid={} ask={}",
                currentSlug.get(),
                side,
                bid,
                ask
        );
    }

    private void handlePriceChange(
            JsonNode root,
            String upToken,
            String downToken) {

        JsonNode changes =
                root.path("price_changes");

        if (!changes.isArray()) {
            return;
        }

        for (JsonNode change : changes) {

            String assetId =
                    change.path("asset_id")
                            .asText(null);

            MarketSide side =
                    mapTokenToSide(
                            assetId,
                            upToken,
                            downToken
                    );

            if (side == null) {
                continue;
            }

            BigDecimal bid =
                    parseDecimal(
                            change.get("best_bid")
                    );

            BigDecimal ask =
                    parseDecimal(
                            change.get("best_ask")
                    );

            updateBestPrices(
                    side,
                    bid,
                    ask
            );

            log.debug(
                    "POLYMARKET PRICE_CHANGE " +
                            "slug={} side={} price={} bid={} ask={}",
                    currentSlug.get(),
                    side,
                    parseDecimal(change.get("price")),
                    bid,
                    ask
            );
        }
    }

    private void handleLastTradePrice(
            JsonNode root,
            String upToken,
            String downToken) {

        String assetId =
                root.path("asset_id")
                        .asText(null);

        MarketSide side =
                mapTokenToSide(
                        assetId,
                        upToken,
                        downToken
                );

        if (side == null) {
            return;
        }

        log.trace(
                "POLYMARKET LAST TRADE side={} price={}",
                side,
                parseDecimal(root.get("price"))
        );
    }

    private MarketSide mapTokenToSide(
            String assetId,
            String upToken,
            String downToken) {

        if (assetId == null) {
            return null;
        }

        if (assetId.equals(upToken)) {
            return MarketSide.UP;
        }

        if (assetId.equals(downToken)) {
            return MarketSide.DOWN;
        }

        return null;
    }

    private BigDecimal findBestBid(
            JsonNode bids) {

        if (!bids.isArray()) {
            return null;
        }

        BigDecimal best = null;

        for (JsonNode level : bids) {

            BigDecimal price =
                    parseDecimal(
                            level.get("price")
                    );

            if (price != null
                    && (best == null
                    || price.compareTo(best) > 0)) {

                best = price;
            }
        }

        return best;
    }

    private BigDecimal findBestAsk(
            JsonNode asks) {

        if (!asks.isArray()) {
            return null;
        }

        BigDecimal best = null;

        for (JsonNode level : asks) {

            BigDecimal price =
                    parseDecimal(
                            level.get("price")
                    );

            if (price != null
                    && (best == null
                    || price.compareTo(best) < 0)) {

                best = price;
            }
        }

        return best;
    }

    private BigDecimal parseDecimal(
            JsonNode node) {

        if (node == null
                || node.isNull()) {

            return null;
        }

        try {

            if (node.isNumber()) {
                return node.decimalValue();
            }

            String value =
                    node.asText();

            if (value == null
                    || value.isBlank()) {

                return null;
            }

            return new BigDecimal(value);

        } catch (Exception e) {

            return null;
        }
    }

    private void updateBestPrices(
            MarketSide side,
            BigDecimal bid,
            BigDecimal ask) {

        if (bid != null) {
            bestBidBySide.put(
                    side,
                    bid
            );
        }

        if (ask != null) {
            bestAskBySide.put(
                    side,
                    ask
            );
        }

        publish(
                side
        );
    }

    private void publish(
            MarketSide side) {

        BigDecimal bid =
                bestBidBySide.get(side);

        BigDecimal ask =
                bestAskBySide.get(side);

        if (bid == null
                || ask == null) {

            return;
        }

        PriceListener current =
                listener;

        if (current == null) {
            return;
        }

        try {

            current.onPriceUpdate(
                    side,
                    bid,
                    ask
            );

        } catch (Exception e) {

            log.error(
                    "Polymarket PriceListener failed",
                    e
            );
        }
    }

    private void sendPing() {

        if (!running) {
            return;
        }

        WebSocketClient client =
                currentClient.get();

        if (client != null
                && client.isOpen()) {

            try {

                client.send("PING");

            } catch (Exception e) {

                log.warn(
                        "Failed to send Polymarket PING",
                        e
                );
            }
        }
    }

    private void checkStaleness() {

        if (!running) {
            return;
        }

        WebSocketClient client =
                currentClient.get();

        if (client == null
                || !client.isOpen()) {

            return;
        }

        if (lastMarketDataMessageAtMillis < 0) {
            return;
        }

        long silentFor =
                System.currentTimeMillis()
                        - lastMarketDataMessageAtMillis;

        if (silentFor >
                STALE_THRESHOLD.toMillis()) {

            log.warn(
                    "POLYMARKET WS STALE: silentFor={}ms. Reconnecting.",
                    silentFor
            );

            connectionGeneration.incrementAndGet();

            closeClient();

            /*
             * Do NOT keep the old prices.
             *
             * They are no longer trustworthy after the connection
             * has gone stale.
             */
            bestBidBySide.clear();
            bestAskBySide.clear();

            scheduleReconnect();
        }
    }

    private synchronized void scheduleReconnect() {

        if (!running) {
            return;
        }

        if (reconnectPending.getAndSet(true)) {
            return;
        }

        taskScheduler.schedule(
                () -> {

                    reconnectPending.set(false);

                    reconnect();

                },
                Instant.now()
                        .plus(RECONNECT_DELAY)
        );
    }

    private void reconnect() {

        if (!running) {
            return;
        }

        String slug =
                currentSlug.get();

        if (slug == null) {
            return;
        }

        WebSocketClient client =
                currentClient.get();

        if (client != null
                && client.isOpen()) {

            return;
        }

        long generation =
                connectionGeneration.incrementAndGet();

        log.info(
                "POLYMARKET WS RECONNECT slug={} generation={}",
                slug,
                generation
        );

        connect(generation);
    }

    private void closeClient() {

        WebSocketClient client =
                currentClient.getAndSet(null);

        if (client != null) {

            try {

                if (!client.isClosed()) {

                    client.close(
                            1000,
                            "Normal closure"
                    );
                }

            } catch (Exception e) {

                log.debug(
                        "Error closing Polymarket WebSocket",
                        e
                );
            }
        }
    }

    private record TokenIds(
            String upTokenId,
            String downTokenId) {
    }
}