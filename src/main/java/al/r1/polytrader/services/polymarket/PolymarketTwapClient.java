package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.config.polymarket.PolymarketProperties;
import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class PolymarketTwapClient {

    private static final Duration PING_INTERVAL = Duration.ofSeconds(5);  // required by RTDS
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);
    private static final BigInteger E18 = BigInteger.TEN.pow(18);

    private final PolymarketProperties properties;
    private final Prices prices;
    private final ProbabilityTable probabilityTable;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;
    private final PolymarketDataProvider marketDataProvider;
    private final TradingEngine tradingEngine;

    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>();
    private final PolymarketRollingWindow rollingWindow = new PolymarketRollingWindow();
    private volatile boolean running = false;

    public PolymarketTwapClient(PolymarketProperties properties,
                                Prices prices,
                                ProbabilityTable probabilityTable,
                                ObjectMapper objectMapper,
                                TaskScheduler liveDataTaskScheduler,
                                PolymarketDataProvider marketDataProvider,
                                TradingEngine tradingEngine) {
        this.properties = properties;
        this.prices = prices;
        this.probabilityTable = probabilityTable;
        this.objectMapper = objectMapper;
        this.taskScheduler = liveDataTaskScheduler;
        this.marketDataProvider = marketDataProvider;
        this.tradingEngine = tradingEngine;
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
        if (!"crypto_prices_twap_sixty".equals(topicNode.stringValue())) return;

        JsonNode payload = node.get("payload");
        if (payload == null) return;

        // filters= already restricts the subscription to btc/usd, but guard
        // anyway in case a future change omits filters to multiplex symbols.
        JsonNode symbolNode = payload.get("symbol");
        if (symbolNode != null && !"btc/usd".equalsIgnoreCase(symbolNode.stringValue())) return;

        JsonNode obsTimestampNode = payload.get("timestamp");
        if (obsTimestampNode == null) {
            log.warn("RTDS TWAP update missing payload.timestamp; skipping rather than guessing an observation time");
            return;
        }
        long observedAtMillis = obsTimestampNode.longValue();

        BigDecimal twapPrice = extractPrice(payload);
        if (twapPrice == null) {
            log.warn("RTDS TWAP update had no usable price field, skipping");
            return;
        }

        log.debug("RTDS 60s TWAP BTC/USD: {} @ {}", twapPrice, observedAtMillis);

        // Store in global Prices
        prices.setPolymarketPrice(twapPrice);

        // Drive the live probability table from Polymarket's own TWAP series
        rollingWindow.addAndUpdateTable(observedAtMillis, twapPrice.doubleValue(), probabilityTable);

        // current market data and run a trade evaluation.
        evaluateTrade();
    }

    private void evaluateTrade() {
        marketDataProvider.currentSnapshot().ifPresentOrElse(this::runDecision,
                () -> log.debug("No open market snapshot yet, skipping trade evaluation"));
    }

    private void runDecision(PolymarketMarketSnapshot snapshot) {
        BigDecimal avg60s = prices.getAvg60sPrice(ChainlinkSymbol.BTC_USD);
        if (avg60s == null || avg60s.signum() == 0) {
            log.debug("No blended 60s average yet, skipping trade evaluation");
            return;
        }

        double requiredPctChange = snapshot.resolutionPrice().subtract(avg60s)
                .divide(avg60s, 8, RoundingMode.HALF_UP).doubleValue();
    }

    private BigDecimal extractPrice(JsonNode payload) {
        JsonNode fullAccuracyNode = payload.get("full_accuracy_value");
        if (fullAccuracyNode != null) {
            try {
                BigInteger raw = new BigInteger(fullAccuracyNode.stringValue());
                return new BigDecimal(raw).divide(new BigDecimal(E18));
            } catch (Exception e) {
                log.warn("Failed to parse full_accuracy_value '{}', falling back to value", fullAccuracyNode);
            }
        }

        JsonNode valueNode = payload.get("value");
        if (valueNode == null) return null;
        try {
            return valueNode.decimalValue();
        } catch (Exception e) {
            log.error("Failed to parse TWAP value node '{}'", valueNode);
            return null;
        }
    }
}