package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams Polymarket's public CLOB market channel for the currently active
 * 5-minute BTC Up/Down market. This channel carries tradable Up/Down token
 * prices, not the BTC/USD value used for market resolution.
 *
 * The BTC/USD settlement price must be read from Chainlink Data Streams using
 * authorized report access; Gamma and the CLOB websocket only expose the
 * resolution-source URL and the outcome-token order book.
 *
 * Extension point: state here is keyed per subscribed token, so adding more
 * assets (ETH 5m, etc.) later is a matter of resolving more token IDs and
 * subscribing them on the same socket — this class isn't hardcoded to BTC
 * beyond which resolver it's given.
 */
@Slf4j
@Service
public class PolymarketWebSocketClient {

    private static final Duration PING_INTERVAL = Duration.ofSeconds(10);
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    private final PolymarketProperties properties;
    private final PolymarketMarketResolver marketResolver;
    private final TaskScheduler taskScheduler;
    private final ObjectMapper objectMapper;

    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private final AtomicReference<String> subscribedUpTokenId = new AtomicReference<>();
    private final AtomicReference<String> subscribedSlug = new AtomicReference<>();

    private volatile boolean running = false;

    public PolymarketWebSocketClient(PolymarketProperties properties,
                                     PolymarketMarketResolver marketResolver,
                                     TaskScheduler liveDataTaskScheduler,
                                     ObjectMapper objectMapper) {
        this.properties = properties;
        this.marketResolver = marketResolver;
        this.taskScheduler = liveDataTaskScheduler;
        this.objectMapper = objectMapper;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        connect();
        taskScheduler.scheduleAtFixedRate(this::resubscribeIfMarketRolled,
                Duration.ofSeconds(properties.marketRefreshSeconds()));
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
        Optional<PolymarketMarketResolver.ResolvedMarket> resolved = marketResolver.resolveCurrentMarket();
        if (resolved.isEmpty()) {
            log.warn("Could not resolve active Polymarket BTC 5m market, retrying in {}", RECONNECT_DELAY);
            taskScheduler.schedule(this::connect, Instant.now().plus(RECONNECT_DELAY));
            return;
        }
        openSocket(resolved.get());
    }

    private void openSocket(PolymarketMarketResolver.ResolvedMarket market) {
        WebSocketClient client = new StandardWebSocketClient();
        subscribedUpTokenId.set(market.upTokenId());
        subscribedSlug.set(market.slug());

        client.execute(new TextWebSocketHandler() {

            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                currentSession.set(session);
                log.info("Connected to Polymarket market channel for {}", market.slug());
                try {
                    String subscribeMsg = objectMapper.writeValueAsString(new SubscribeMessage(
                            List.of(market.upTokenId(), market.downTokenId()), "market", true));
                    session.sendMessage(new TextMessage(subscribeMsg));
                } catch (Exception e) {
                    log.error("Failed to send Polymarket subscribe message", e);
                }
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) {
                String payload = message.getPayload();
                if ("PONG".equals(payload)) return;
                try {
                    onMarketEvent(objectMapper.readTree(payload));
                } catch (Exception e) {
                    log.error("Failed to process Polymarket market message", e);
                }
            }

            @Override
            public void handleTransportError(WebSocketSession session, Throwable exception) {
                log.warn("Polymarket websocket transport error, reconnecting", exception);
                scheduleReconnect();
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) {
                log.warn("Polymarket websocket closed ({}), reconnecting", closeStatus);
                currentSession.compareAndSet(session, null);
                scheduleReconnect();
            }
        }, properties.wssMarketUrl());
    }

    private void scheduleReconnect() {
        if (!running) return;
        taskScheduler.schedule(this::connect, Instant.now().plus(RECONNECT_DELAY));
    }

    private void resubscribeIfMarketRolled() {
        if (!running) return;
        Optional<PolymarketMarketResolver.ResolvedMarket> resolved = marketResolver.resolveCurrentMarket();
        if (resolved.isEmpty()) return;
        if (!resolved.get().slug().equals(subscribedSlug.get())) {
            log.info("Polymarket 5m window rolled over, resubscribing to {}", resolved.get().slug());
            WebSocketSession old = currentSession.getAndSet(null);
            if (old != null && old.isOpen()) {
                try { old.close(CloseStatus.NORMAL); } catch (Exception ignored) { }
            }
            openSocket(resolved.get());
        }
    }

    private void sendPing() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            try {
                session.sendMessage(new TextMessage("PING"));
            } catch (Exception e) {
                log.warn("Failed to send Polymarket PING", e);
            }
        }
    }

    private void onMarketEvent(JsonNode node) {
        JsonNode eventTypeNode = node.get("event_type");
        if (eventTypeNode == null) return;

        Double mid = switch (eventTypeNode.stringValue()) {
            case "book" -> midFromBook(node);
            case "price_change" -> midFromPriceChange(node);
            case "last_trade_price" -> lastTradePrice(node);
            default -> null; // tick_size_change etc. — not needed for pricing
        };

        if (mid != null) {
            // Up/Down token prices are intentionally not published as BTC/USD.
            // They are probabilities expressed in dollars per outcome token.
        }
    }

    private Double midFromBook(JsonNode node) {
        JsonNode assetId = node.get("asset_id");
        if (assetId == null || !assetId.stringValue().equals(subscribedUpTokenId.get())) return null;
        Double bestBid = topOfBook(node.get("bids"), true);
        Double bestAsk = topOfBook(node.get("asks"), false);
        if (bestBid == null || bestAsk == null) return null;
        return (bestBid + bestAsk) / 2.0;
    }

    private Double topOfBook(JsonNode levels, boolean wantHighest) {
        if (levels == null) return null;
        Double best = null;
        for (JsonNode level : levels) {
            JsonNode priceNode = level.get("price");
            if (priceNode == null) continue;
            double price = new BigDecimal(priceNode.stringValue()).doubleValue();
            if (best == null || (wantHighest ? price > best : price < best)) best = price;
        }
        return best;
    }

    private Double midFromPriceChange(JsonNode node) {
        JsonNode changes = node.get("price_changes");
        if (changes == null) return null;
        for (JsonNode change : changes) {
            JsonNode assetId = change.get("asset_id");
            if (assetId == null || !assetId.stringValue().equals(subscribedUpTokenId.get())) continue;
            JsonNode bestBid = change.get("best_bid");
            JsonNode bestAsk = change.get("best_ask");
            if (bestBid == null || bestAsk == null) continue;
            double bid = new BigDecimal(bestBid.stringValue()).doubleValue();
            double ask = new BigDecimal(bestAsk.stringValue()).doubleValue();
            return (bid + ask) / 2.0;
        }
        return null;
    }

    private Double lastTradePrice(JsonNode node) {
        JsonNode assetId = node.get("asset_id");
        if (assetId == null || !assetId.stringValue().equals(subscribedUpTokenId.get())) return null;
        JsonNode priceNode = node.get("price");
        if (priceNode == null) return null;
        return new BigDecimal(priceNode.stringValue()).doubleValue();
    }

    private record SubscribeMessage(List<String> assets_ids, String type, boolean custom_feature_enabled) {}
}
