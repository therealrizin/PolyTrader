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

    private static final Duration PING_INTERVAL =
            Duration.ofSeconds(5);

    private static final Duration RECONNECT_DELAY =
            Duration.ofSeconds(5);

    private static final Duration STALE_THRESHOLD =
            Duration.ofSeconds(20);

    private static final Duration STALENESS_CHECK_INTERVAL =
            Duration.ofSeconds(5);

    private static final String RAW_TOPIC =
            "crypto_prices_chainlink";

    private static final String TWAP_SIXTY_TOPIC =
            "crypto_prices_twap_sixty";

    private static final BigInteger E18 =
            BigInteger.TEN.pow(18);

    private final PolymarketProperties properties;
    private final ObjectMapper objectMapper;
    private final TaskScheduler taskScheduler;
    private final Prices prices;
    private final ProbabilityTable probabilityTable;

    private final PolymarketRollingWindow btcRollingWindow =
            new PolymarketRollingWindow();

    private final AtomicReference<WebSocketSession> currentSession =
            new AtomicReference<>();

    private volatile WebSocketClient client;

    private volatile long lastMessageAtMillis = -1;

    private volatile boolean running = false;

    public ChainlinkPriceStreamClient(
            PolymarketProperties properties,
            ObjectMapper objectMapper,
            TaskScheduler taskScheduler,
            Prices prices,
            ProbabilityTable probabilityTable
    ) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.taskScheduler = taskScheduler;
        this.prices = prices;
        this.probabilityTable = probabilityTable;
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;

        connect();

        taskScheduler.scheduleAtFixedRate(
                this::sendPing,
                PING_INTERVAL
        );

        taskScheduler.scheduleAtFixedRate(
                this::checkStaleness,
                STALENESS_CHECK_INTERVAL
        );

        log.info(
                "Chainlink RTDS stream started: url={}",
                properties.wssLiveDataUrl()
        );
    }

    public synchronized void stop() {
        running = false;

        WebSocketSession session =
                currentSession.getAndSet(null);

        if (session != null
                && session.isOpen()) {

            try {
                session.close(
                        CloseStatus.NORMAL
                );
            } catch (Exception e) {
                log.debug(
                        "Failed to close Chainlink RTDS session",
                        e
                );
            }
        }

        log.info(
                "Chainlink RTDS stream stopped"
        );
    }

    private void connect() {
        if (!running) {
            return;
        }

        client =
                new StandardWebSocketClient();

        String url =
                properties.wssLiveDataUrl();

        client.execute(
                new TextWebSocketHandler() {

                    @Override
                    public void afterConnectionEstablished(
                            WebSocketSession session
                    ) {
                        currentSession.set(session);

                        lastMessageAtMillis =
                                System.currentTimeMillis();

                        log.info(
                                "Connected to Polymarket RTDS: session={}",
                                session.getId()
                        );

                        sendSubscription(session);
                    }

                    @Override
                    protected void handleTextMessage(
                            WebSocketSession session,
                            TextMessage message
                    ) {
                        String payload =
                                message.getPayload();

                        if ("PONG".equals(payload)
                                || "PING".equals(payload)) {

                            return;
                        }

                        /*
                         * This timestamp only tracks RTDS socket activity.
                         * It is NOT used for price freshness.
                         */
                        lastMessageAtMillis =
                                System.currentTimeMillis();

                        try {
                            onMessage(
                                    objectMapper.readTree(
                                            payload
                                    )
                            );

                        } catch (Exception e) {

                            log.warn(
                                    "Failed to parse Polymarket RTDS message: {}",
                                    payload,
                                    e
                            );
                        }
                    }

                    @Override
                    public void handleTransportError(
                            WebSocketSession session,
                            Throwable exception
                    ) {
                        currentSession.compareAndSet(
                                session,
                                null
                        );

                        log.warn(
                                "Polymarket RTDS transport error",
                                exception
                        );

                        scheduleReconnect();
                    }

                    @Override
                    public void afterConnectionClosed(
                            WebSocketSession session,
                            CloseStatus status
                    ) {
                        currentSession.compareAndSet(
                                session,
                                null
                        );

                        log.warn(
                                "Polymarket RTDS connection closed: status={}",
                                status
                        );

                        if (running) {
                            scheduleReconnect();
                        }
                    }
                },
                url
        );
    }

    private void sendSubscription(
            WebSocketSession session
    ) {
        try {
            List<Map<String, Object>> subscriptions =
                    new ArrayList<>();

            for (ChainlinkSymbol symbol :
                    ChainlinkSymbol.values()) {

                String filter =
                        "{\"symbol\":\""
                                + symbol.getWire()
                                + "\"}";

                subscriptions.add(
                        Map.of(
                                "topic",
                                RAW_TOPIC,
                                "type",
                                "*",
                                "filters",
                                filter
                        )
                );

                subscriptions.add(
                        Map.of(
                                "topic",
                                TWAP_SIXTY_TOPIC,
                                "type",
                                "update",
                                "filters",
                                filter
                        )
                );
            }

            String subscribeMsg =
                    objectMapper.writeValueAsString(
                            Map.of(
                                    "action",
                                    "subscribe",
                                    "subscriptions",
                                    subscriptions
                            )
                    );

            session.sendMessage(
                    new TextMessage(
                            subscribeMsg
                    )
            );

            log.info(
                    "Subscribed to Polymarket Chainlink RTDS topics: {}",
                    subscriptions
            );

        } catch (Exception e) {

            log.error(
                    "Failed to subscribe to Polymarket RTDS",
                    e
            );
        }
    }

    private void onMessage(
            JsonNode node
    ) {
        String topic =
                node.path("topic")
                        .asText("");

        if (topic.isBlank()) {
            return;
        }

        JsonNode payload =
                node.get("payload");

        if (payload == null) {
            return;
        }

        ChainlinkSymbol symbol =
                ChainlinkSymbol.fromWire(
                        payload.path("symbol")
                                .asText(null)
                );

        if (symbol == null) {
            return;
        }

        switch (topic) {

            case RAW_TOPIC ->
                    handleRawPrice(
                            symbol,
                            payload
                    );

            case TWAP_SIXTY_TOPIC ->
                    handleTwapSixty(
                            symbol,
                            payload
                    );

            default ->
                    log.debug(
                            "Ignoring unknown Polymarket RTDS topic: {}",
                            topic
                    );
        }
    }

    private void handleRawPrice(
            ChainlinkSymbol symbol,
            JsonNode payload
    ) {
        BigDecimal price =
                parseDecimal(
                        payload.get("value")
                );

        if (price == null) {
            return;
        }

        JsonNode timestampNode =
                payload.get("timestamp");

        /*
         * NEVER update the raw price without a timestamp.
         *
         * This prevents an untrusted/untimestamped update from changing
         * the price while leaving the freshness state ambiguous.
         */
        if (timestampNode == null
                || timestampNode.isNull()) {

            log.warn(
                    "Chainlink raw price missing payload.timestamp: symbol={} payload={}",
                    symbol.getWire(),
                    payload
            );

            return;
        }

        long observedAtMillis =
                timestampNode.longValue();

        if (observedAtMillis <= 0) {
            log.warn(
                    "Chainlink raw price has invalid payload.timestamp: symbol={} timestamp={} payload={}",
                    symbol.getWire(),
                    observedAtMillis,
                    payload
            );

            return;
        }

        /*
         * Accept Unix seconds as well as Unix milliseconds.
         */
        if (observedAtMillis < 100_000_000_000L) {
            observedAtMillis *= 1000L;
        }

        /*
         * Prices.updatePrice() records BOTH:
         *
         * 1. observedAtMillis = timestamp from Polymarket RTDS
         * 2. receivedAtMillis = System.currentTimeMillis()
         *
         * The latter is what the 500 ms safety check uses.
         */
        prices.updatePrice(
                symbol,
                price,
                observedAtMillis
        );

        log.trace(
                "Chainlink raw price: symbol={} price={} observedAt={} receivedAt={} ageMs={}",
                symbol.getWire(),
                price,
                Instant.ofEpochMilli(
                        observedAtMillis
                ),
                Instant.now(),
                prices.getPriceAgeMillis(symbol)
        );
    }

    private void handleTwapSixty(
            ChainlinkSymbol symbol,
            JsonNode payload
    ) {
        BigDecimal twap =
                parseTwapValue(
                        payload
                );

        if (twap == null) {
            return;
        }

        prices.updateAvg60sPrice(
                symbol,
                twap
        );

        if (symbol == ChainlinkSymbol.BTC_USD) {

            JsonNode obsTimestampNode =
                    payload.get("timestamp");

            if (obsTimestampNode == null
                    || obsTimestampNode.isNull()) {

                log.warn(
                        "Chainlink TWAP missing payload.timestamp: symbol={} payload={}",
                        symbol.getWire(),
                        payload
                );

                return;
            }

            long observedAtMillis =
                    obsTimestampNode.longValue();

            if (observedAtMillis <= 0) {
                log.warn(
                        "Chainlink TWAP has invalid payload.timestamp: symbol={} timestamp={} payload={}",
                        symbol.getWire(),
                        observedAtMillis,
                        payload
                );

                return;
            }

            if (observedAtMillis < 100_000_000_000L) {
                observedAtMillis *= 1000L;
            }

            btcRollingWindow.addAndUpdateTable(
                    observedAtMillis,
                    twap.doubleValue(),
                    probabilityTable
            );
        }
    }

    private BigDecimal parseTwapValue(
            JsonNode payload
    ) {
        JsonNode fullAccuracyNode =
                payload.get("full_accuracy_value");

        if (fullAccuracyNode != null
                && !fullAccuracyNode.isNull()) {

            try {
                BigInteger raw =
                        new BigInteger(
                                fullAccuracyNode.stringValue()
                        );

                return new BigDecimal(raw)
                        .divide(
                                new BigDecimal(E18)
                        );

            } catch (Exception e) {

                log.warn(
                        "Failed to parse Chainlink TWAP full_accuracy_value: {}",
                        fullAccuracyNode,
                        e
                );
            }
        }

        return parseDecimal(
                payload.get("value")
        );
    }

    private BigDecimal parseDecimal(
            JsonNode node
    ) {
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

            return new BigDecimal(
                    value
            );

        } catch (Exception e) {

            log.warn(
                    "Failed to parse decimal RTDS value: {}",
                    node,
                    e
            );

            return null;
        }
    }

    private void sendPing() {
        if (!running) {
            return;
        }

        WebSocketSession session =
                currentSession.get();

        if (session == null
                || !session.isOpen()) {

            return;
        }

        try {
            session.sendMessage(
                    new TextMessage("PING")
            );

        } catch (Exception e) {

            log.warn(
                    "Failed to send RTDS PING",
                    e
            );
        }
    }

    private void checkStaleness() {
        if (!running
                || lastMessageAtMillis < 0) {

            return;
        }

        long silentFor =
                System.currentTimeMillis()
                        - lastMessageAtMillis;

        if (silentFor
                > STALE_THRESHOLD.toMillis()) {

            log.warn(
                    "Polymarket RTDS socket appears stale: silentForMs={} thresholdMs={}",
                    silentFor,
                    STALE_THRESHOLD.toMillis()
            );

            WebSocketSession session =
                    currentSession.getAndSet(null);

            if (session != null
                    && session.isOpen()) {

                try {
                    session.close(
                            CloseStatus.GOING_AWAY
                    );

                } catch (Exception e) {

                    log.debug(
                            "Failed to close stale RTDS session",
                            e
                    );
                }
            }

            lastMessageAtMillis =
                    System.currentTimeMillis();

            connect();
        }
    }

    private void scheduleReconnect() {
        if (!running) {
            return;
        }

        taskScheduler.schedule(
                this::connect,
                Instant.now()
                        .plus(RECONNECT_DELAY)
        );
    }
}
