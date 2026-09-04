package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.ExecutionOrderRequest;
import al.r1.polytrader.services.betting.model.ExecutionOrderResponse;
import al.r1.polytrader.services.betting.model.RealBet;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketMarketResolver;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class RealBetService {

    private static final ChainlinkSymbol SYMBOL =
            ChainlinkSymbol.BTC_USD;

    /*
     * Leave a tiny amount of the recorded position unsold.
     *
     * This protects against tiny discrepancies between the local
     * recorded position and the actual CLOB balance.
     */
    private static final BigDecimal SELL_SAFETY_MARGIN =
            new BigDecimal("0.995");

    /*
     * BTC 5m markets normally use 0.01 tick pricing.
     *
     * The executor/CLOB determines the actual market tick.
     * This class intentionally never rounds UP a BUY price.
     */
    private static final int PRICE_SCALE = 2;

    /*
     * FOK/FAK market-order taker amount supports up to 4 decimals.
     */
    private static final int BUY_SIZE_SCALE = 4;

    /*
     * Dollar amount / maker amount is limited to cents.
     */
    private static final int USDC_SCALE = 2;

    /*
     * Polymarket minimum order value.
     */
    private static final BigDecimal MIN_ORDER_USDC =
            new BigDecimal("1.00");

    private final TradingProperties tradingProperties;
    private final PolymarketMarketResolver marketResolver;
    private final WebClient executionWebClient;
    private final Prices prices;
    private final TradingEngine tradingEngine;

    private final ObjectMapper objectMapper =
            new ObjectMapper();

    private final Set<String> openSlugs =
            ConcurrentHashMap.newKeySet();

    private final Map<String, RealBet> bets =
            new ConcurrentHashMap<>();

    public RealBetService(
            TradingProperties tradingProperties,
            PolymarketMarketResolver marketResolver,
            @Qualifier("executionWebClient")
            WebClient executionWebClient,
            Prices prices,
            TradingEngine tradingEngine) {

        this.tradingProperties = tradingProperties;
        this.marketResolver = marketResolver;
        this.executionWebClient = executionWebClient;
        this.prices = prices;
        this.tradingEngine = tradingEngine;
    }

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public RealBet placeRealBet(
            PolymarketMarketSnapshot snapshot,
            MarketSide side,
            double countedEv,
            double countedWinChance) {

        BigDecimal price =
                getExecutablePrice(snapshot, side);

        return placeRealBetAtPrice(
                snapshot,
                side,
                price,
                countedEv,
                countedWinChance
        );
    }

    /**
     * Places a BUY FOK.
     *
     * IMPORTANT:
     *
     * FOK/FAK BUY orders are handled by the CLOB as market-order
     * amount orders.
     *
     * Therefore:
     *
     *   amount_usdc = canonical dollar amount
     *   size        = estimated number of shares
     *
     * The executor MUST use amount_usdc as the BUY maker amount.
     *
     * We NEVER construct:
     *
     *   amount_usdc = price * size
     *
     * because that can create fractional-cent maker amounts such as:
     *
     *   0.65 * 1.54 = 1.0010
     *
     * which the CLOB rejects for FOK/FAK.
     */
    public RealBet placeRealBetAtPrice(
            PolymarketMarketSnapshot snapshot,
            MarketSide side,
            BigDecimal maxAcceptablePrice,
            double countedEv,
            double countedWinChance) {

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Polymarket snapshot cannot be null"
            );
        }

        String slug = snapshot.slug();

        if (slug == null || slug.isBlank()) {
            throw new IllegalArgumentException(
                    "Polymarket market slug cannot be empty"
            );
        }

        /*
         * Only one open position per market.
         */
        if (!openSlugs.add(slug)) {
            throw new IllegalStateException(
                    "Already have an open real bet for market " + slug
            );
        }

        try {

            /*
             * Never trade using stale Chainlink data.
             */
            requireFreshPrice(slug);

            validatePrice(maxAcceptablePrice);

            /*
             * BUY limit price is a maximum price.
             *
             * Therefore ALWAYS round DOWN.
             */
            BigDecimal tickSafePrice =
                    maxAcceptablePrice.setScale(
                            PRICE_SCALE,
                            RoundingMode.DOWN
                    );

            if (tickSafePrice.signum() <= 0) {
                throw new FokNotFilledException(
                        "Max acceptable price "
                                + maxAcceptablePrice
                                + " rounds to zero"
                );
            }

            BigDecimal configuredAmount =
                    tradingProperties.betAmount();

            if (configuredAmount == null
                    || configuredAmount.signum() <= 0) {

                throw new IllegalStateException(
                        "trading.bet-amount must be > 0"
                );
            }

            /*
             * Polymarket minimum.
             *
             * If configured amount is below $1, do NOT silently
             * increase it. The correct behavior is to reject.
             */
            if (configuredAmount.compareTo(MIN_ORDER_USDC) < 0) {

                throw new IllegalStateException(
                        "Configured bet amount "
                                + configuredAmount
                                + " is below Polymarket minimum "
                                + MIN_ORDER_USDC
                );
            }

            /*
             * BUY maker amount is canonical.
             *
             * Round DOWN so we NEVER spend more than configured.
             */
            BigDecimal amountUsdc =
                    configuredAmount.setScale(
                            USDC_SCALE,
                            RoundingMode.DOWN
                    );

            if (amountUsdc.compareTo(MIN_ORDER_USDC) < 0) {
                throw new IllegalStateException(
                        "Configured bet amount becomes "
                                + amountUsdc
                                + " after cent rounding"
                );
            }

            /*
             * Estimate taker/share amount.
             *
             * This is NOT used to derive the dollar amount.
             *
             * For:
             *
             *   $1.00 / $0.65
             *
             * we get:
             *
             *   1.538461...
             *
             * and submit:
             *
             *   1.5384
             *
             * The Rust executor should use amount_usdc as the
             * authoritative BUY amount and derive its signed
             * taker amount according to the CLOB rules.
             */
            BigDecimal estimatedSize =
                    amountUsdc.divide(
                            tickSafePrice,
                            BUY_SIZE_SCALE,
                            RoundingMode.DOWN
                    );

            if (estimatedSize.signum() <= 0) {
                throw new IllegalStateException(
                        "Calculated BUY size is zero: "
                                + "amount="
                                + amountUsdc
                                + ", price="
                                + tickSafePrice
                );
            }

            /*
             * The actual economic amount we intentionally authorize
             * is amountUsdc.
             *
             * DO NOT calculate:
             *
             * amountUsdc = price * size
             *
             * because that would create fractional-cent amounts.
             */
            String clientBetId =
                    UUID.randomUUID().toString();

            /*
             * Resolve the current market/token.
             */
            PolymarketMarketResolver.ResolvedMarket market =
                    marketResolver.resolveCurrentMarket()
                            .filter(m -> slug.equals(m.slug()))
                            .orElseThrow(() ->
                                    new IllegalStateException(
                                            "Could not resolve CLOB token IDs for "
                                                    + slug
                                    ));

            String tokenId =
                    side == MarketSide.UP
                            ? market.upTokenId()
                            : market.downTokenId();

            if (tokenId == null || tokenId.isBlank()) {
                throw new IllegalStateException(
                        "Resolved token ID is empty for "
                                + slug
                                + " / "
                                + side
                );
            }

            /*
             * Last-second safety check.
             */
            requireFreshPrice(slug);

            /*
             * IMPORTANT:
             *
             * amount_usdc is authoritative for BUY.
             *
             * size is only the corresponding estimated taker amount.
             */
            ExecutionOrderRequest request =
                    new ExecutionOrderRequest(
                            clientBetId,
                            slug,
                            tokenId,
                            "BUY",
                            tickSafePrice.toPlainString(),
                            estimatedSize.toPlainString(),
                            amountUsdc.toPlainString(),
                            "FOK",
                            null
                    );

            logBuyRequest(
                    request,
                    clientBetId,
                    slug,
                    side,
                    tickSafePrice,
                    estimatedSize,
                    amountUsdc,
                    configuredAmount,
                    countedEv,
                    countedWinChance,
                    snapshot
            );

            ExecutionOrderResponse response;

            try {

                response =
                        executionWebClient.post()
                                .uri("/order")
                                .bodyValue(request)
                                .retrieve()
                                .bodyToMono(
                                        ExecutionOrderResponse.class
                                )
                                .block(
                                        Duration.ofSeconds(10)
                                );

            } catch (WebClientResponseException e) {

                String errorBody =
                        e.getResponseBodyAsString();

                log.error(
                        "Executor BUY rejected: status={} body={}",
                        e.getStatusCode(),
                        errorBody
                );

                throw new IllegalStateException(
                        "Executor rejected BUY order: "
                                + errorBody,
                        e
                );
            }

            if (response == null) {
                throw new IllegalStateException(
                        "Execution service returned empty BUY response"
                );
            }

            if (!response.success()) {

                log.info(
                        "REAL FOK BUY NOT FILLED: id={} slug={} side={} price={} amountUsdc={} estimatedSize={} error={}",
                        clientBetId,
                        slug,
                        side,
                        tickSafePrice,
                        amountUsdc,
                        estimatedSize,
                        response.error()
                );

                throw new FokNotFilledException(
                        response.error()
                );
            }

            if (response.orderId() == null
                    || response.orderId().isBlank()) {

                throw new IllegalStateException(
                        "BUY succeeded but no order_id returned"
                );
            }

            /*
             * The executor should return the actual filled taker/share
             * amount.
             */
            BigDecimal actualSize =
                    response.takingAmount() != null
                            && response.takingAmount().signum() > 0
                            ? response.takingAmount()
                            : estimatedSize;

            if (actualSize.signum() <= 0) {
                throw new FokNotFilledException(
                        "FOK returned zero filled size"
                );
            }

            Instant placedAt =
                    Instant.now();

            /*
             * IMPORTANT ACCOUNTING NOTE:
             *
             * bet.amount() is the authorized BUY dollar amount.
             *
             * For a market BUY this is the correct canonical amount
             * passed to the executor.
             */
            RealBet bet =
                    new RealBet(
                            clientBetId,
                            response.orderId(),
                            tokenId,
                            slug,
                            side,
                            amountUsdc,
                            tickSafePrice,
                            actualSize,
                            countedEv,
                            countedWinChance,
                            placedAt,
                            snapshot.secondsUntilClose(),
                            BetStatus.OPEN,
                            null,
                            null,
                            null
                    );

            bets.put(
                    clientBetId,
                    bet
            );

            log.info(
                    "REAL FOK BUY FILLED: id={} orderId={} slug={} side={} price={} amountUsdc={} recordedSize={} EV={} winChance={} secondsLeft={}",
                    clientBetId,
                    response.orderId(),
                    slug,
                    side,
                    tickSafePrice,
                    amountUsdc,
                    actualSize,
                    countedEv,
                    countedWinChance,
                    snapshot.secondsUntilClose()
            );

            return bet;

        } catch (Exception e) {

            /*
             * A position was not successfully recorded.
             *
             * Free the market slot.
             */
            openSlugs.remove(slug);

            if (e instanceof FokNotFilledException) {

                log.info(
                        "REAL FOK BUY DID NOT FILL: slug={} side={} reason={}",
                        slug,
                        side,
                        e.getMessage()
                );

            } else {

                log.error(
                        "REAL FOK BUY FAILED: slug={} side={}",
                        slug,
                        side,
                        e
                );
            }

            throw e;
        }
    }

    private void logBuyRequest(
            ExecutionOrderRequest request,
            String clientBetId,
            String slug,
            MarketSide side,
            BigDecimal price,
            BigDecimal size,
            BigDecimal amountUsdc,
            BigDecimal configuredAmount,
            double countedEv,
            double countedWinChance,
            PolymarketMarketSnapshot snapshot) {

        try {

            log.info(
                    "REAL FOK BUY REQUEST JSON: {}",
                    objectMapper.writeValueAsString(request)
            );

        } catch (Exception e) {

            log.warn(
                    "Failed to serialize BUY request JSON",
                    e
            );
        }

        log.info(
                "REAL FOK BUY submitting: id={} slug={} side={} tokenId={} price={} estimatedSize={} amountUsdc={} configuredAmount={} EV={} winChance={} secondsUntilClose={} priceAgeMs={}",
                clientBetId,
                slug,
                side,
                request.tokenId(),
                price,
                size,
                amountUsdc,
                configuredAmount,
                countedEv,
                countedWinChance,
                snapshot.secondsUntilClose(),
                prices.getPriceAgeMillis(SYMBOL)
        );
    }

    private void requireFreshPrice(String slug) {

        long observedAtMillis =
                prices.getLastPriceTimestampMillis(SYMBOL);

        long ageMillis =
                prices.getPriceAgeMillis(SYMBOL);

        BigDecimal currentPrice =
                prices.getPrice(SYMBOL);

        if (observedAtMillis <= 0) {

            log.warn(
                    "REAL BET BLOCKED: slug={} reason=NO_RTDS_PRICE symbol={} currentPrice={} lastTimestamp={}",
                    slug,
                    SYMBOL,
                    currentPrice,
                    observedAtMillis
            );

            throw new IllegalStateException(
                    "Cannot place real bet: no timestamped "
                            + "Polymarket RTDS Chainlink price available"
            );
        }

        if (!prices.isPriceFresh(SYMBOL)) {

            log.warn(
                    "REAL BET BLOCKED: slug={} reason=STALE_RTDS_PRICE symbol={} currentPrice={} ageMs={} maxAgeMs={} observedAt={} now={}",
                    slug,
                    SYMBOL,
                    currentPrice,
                    ageMillis,
                    Prices.MAX_PRICE_AGE_MILLIS,
                    Instant.ofEpochMilli(observedAtMillis),
                    Instant.now()
            );

            throw new IllegalStateException(
                    "Cannot place real bet: Chainlink RTDS "
                            + "price is stale. Age="
                            + ageMillis
                            + "ms, maximum="
                            + Prices.MAX_PRICE_AGE_MILLIS
                            + "ms"
            );
        }
    }

    public synchronized Optional<RealBet> sellOpenPosition(
            PolymarketMarketSnapshot snapshot) {

        if (snapshot == null) {
            return Optional.empty();
        }

        String slug = snapshot.slug();

        if (slug == null
                || !openSlugs.contains(slug)) {

            return Optional.empty();
        }

        if (!prices.isPriceFresh(SYMBOL)) {

            log.info(
                    "SELL_CHECK (REAL) slug={} -> hold (Chainlink stale, ageMs={})",
                    slug,
                    prices.getPriceAgeMillis(SYMBOL)
            );

            return Optional.empty();
        }

        RealBet bet =
                findOpenBetForSlug(slug);

        if (bet == null) {
            return Optional.empty();
        }

        BigDecimal currentBid =
                bet.side() == MarketSide.UP
                        ? snapshot.upBid()
                        : snapshot.downBid();

        if (currentBid == null
                || currentBid.signum() <= 0
                || currentBid.compareTo(BigDecimal.ONE) >= 0) {

            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} -> hold (invalid live bid)",
                    slug,
                    bet.id(),
                    bet.side(),
                    currentBid
            );

            return Optional.empty();
        }

        BigDecimal currentLivePrice =
                prices.getPrice(SYMBOL);

        BigDecimal currentTwapPrice =
                prices.getAvg60sPrice(SYMBOL);

        BigDecimal strike =
                snapshot.strikePriceUsd();

        if (currentLivePrice == null
                || currentLivePrice.signum() <= 0
                || currentTwapPrice == null
                || currentTwapPrice.signum() <= 0
                || strike == null
                || strike.signum() <= 0) {

            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} -> hold (price model unavailable)",
                    slug,
                    bet.id(),
                    bet.side(),
                    currentBid
            );

            return Optional.empty();
        }

        var estimate =
                tradingEngine.estimateUpDown(
                        currentLivePrice,
                        currentTwapPrice,
                        strike,
                        (int) Math.max(
                                0,
                                snapshot.secondsUntilClose()
                        ),
                        snapshot.upPrice() != null
                                ? snapshot.upPrice().doubleValue()
                                : 0.5,
                        snapshot.downPrice() != null
                                ? snapshot.downPrice().doubleValue()
                                : 0.5,
                        tradingProperties.takerFee()
                );

        double winChance =
                bet.side() == MarketSide.UP
                        ? estimate.upChance()
                        : estimate.downChance();

        double holdValue =
                tradingEngine.holdValuePerShare(
                        winChance
                );

        double sellValue =
                tradingEngine.netSellValuePerShare(
                        currentBid.doubleValue(),
                        tradingProperties.takerFee()
                );

        double sellingEv =
                sellValue - holdValue;

        double currentReturn =
                bet.price().signum() > 0
                        ? currentBid
                        .divide(
                                bet.price(),
                                8,
                                RoundingMode.HALF_UP
                        )
                        .doubleValue()
                        - 1.0
                        : 0.0;

        double sellThreshold =
                tradingProperties.minimumExpectedEv()
                        / 3.0;

        log.info(
                "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} winChance={} holdValue={} sellValue={} sellingEv={} threshold={} currentReturn={} secondsLeft={}",
                slug,
                bet.id(),
                bet.side(),
                currentBid,
                round(winChance),
                round(holdValue),
                round(sellValue),
                round(sellingEv),
                round(sellThreshold),
                round(currentReturn),
                snapshot.secondsUntilClose()
        );

        if (sellingEv < sellThreshold) {
            return Optional.empty();
        }

        try {

            return Optional.of(
                    executeSell(
                            bet,
                            currentBid,
                            sellingEv,
                            winChance,
                            holdValue,
                            sellValue,
                            sellThreshold
                    )
            );

        } catch (Exception e) {

            log.error(
                    "REAL SELL FAILED: id={} slug={} side={}",
                    bet.id(),
                    slug,
                    bet.side(),
                    e
            );

            return Optional.empty();
        }
    }

    private RealBet findOpenBetForSlug(
            String slug) {

        for (RealBet bet : bets.values()) {

            if (bet.status() == BetStatus.OPEN
                    && slug.equals(bet.marketSlug())) {

                return bet;
            }
        }

        return null;
    }

    private RealBet executeSell(
            RealBet bet,
            BigDecimal currentBid,
            double sellingEv,
            double winChance,
            double holdValue,
            double sellValue,
            double sellThreshold) {

        String slug =
                bet.marketSlug();

        /*
         * Keep a tiny reserve to protect against local-vs-CLOB
         * position-size discrepancies.
         */
        BigDecimal sellSize =
                bet.size()
                        .multiply(SELL_SAFETY_MARGIN)
                        .setScale(
                                4,
                                RoundingMode.DOWN
                        );

        if (sellSize.signum() <= 0) {
            throw new IllegalStateException(
                    "Position size "
                            + bet.size()
                            + " rounds down to zero sellable shares"
            );
        }

        /*
         * SELL FOK:
         *
         * price = minimum acceptable execution price.
         *
         * The executor MUST NOT sell below this price.
         */
        BigDecimal sellPrice =
                currentBid.setScale(
                        PRICE_SCALE,
                        RoundingMode.DOWN
                );

        if (sellPrice.signum() <= 0
                || sellPrice.compareTo(BigDecimal.ONE) >= 0) {

            throw new IllegalStateException(
                    "Invalid SELL price after rounding: "
                            + sellPrice
            );
        }

        /*
         * SELL amount is the number of shares.
         *
         * amount_usdc is informational / executor-compatible.
         */
        BigDecimal amountUsdc =
                sellSize
                        .multiply(sellPrice)
                        .setScale(
                                USDC_SCALE,
                                RoundingMode.DOWN
                        );

        String clientOrderId =
                UUID.randomUUID().toString();

        ExecutionOrderRequest request =
                new ExecutionOrderRequest(
                        clientOrderId,
                        slug,
                        bet.tokenId(),
                        "SELL",
                        sellPrice.toPlainString(),
                        sellSize.toPlainString(),
                        amountUsdc.toPlainString(),
                        "FOK",
                        sellSize.toPlainString()
                );

        try {

            log.info(
                    "REAL SELL REQUEST JSON: {}",
                    objectMapper.writeValueAsString(request)
            );

        } catch (Exception e) {

            log.warn(
                    "Failed to serialize SELL request JSON",
                    e
            );
        }

        BigDecimal netProceeds =
                netSellProceeds(
                        sellSize,
                        sellPrice
                );

        BigDecimal netProfitIfSold =
                netProceeds.subtract(
                        bet.amount()
                );

        ExecutionOrderResponse response;

        try {

            response =
                    executionWebClient.post()
                            .uri("/order")
                            .bodyValue(request)
                            .retrieve()
                            .bodyToMono(
                                    ExecutionOrderResponse.class
                            )
                            .block(
                                    Duration.ofSeconds(10)
                            );

        } catch (WebClientResponseException e) {

            String errorBody =
                    e.getResponseBodyAsString();

            throw new IllegalStateException(
                    "Executor rejected SELL order: "
                            + errorBody,
                    e
            );
        }

        if (response == null) {
            throw new IllegalStateException(
                    "Execution service returned empty SELL response"
            );
        }

        if (!response.success()) {
            throw new IllegalStateException(
                    "Polymarket SELL rejected: "
                            + response.error()
            );
        }

        if (response.orderId() == null
                || response.orderId().isBlank()) {

            throw new IllegalStateException(
                    "SELL succeeded but no order_id returned"
            );
        }

        /*
         * Only mark the position SOLD after the executor reports
         * success.
         */
        RealBet sold =
                new RealBet(
                        bet.id(),
                        bet.orderId(),
                        bet.tokenId(),
                        slug,
                        bet.side(),
                        bet.amount(),
                        bet.price(),
                        bet.size(),
                        bet.countedEv(),
                        bet.countedWinChance(),
                        bet.placedAt(),
                        bet.secondsUntilClose(),
                        BetStatus.SOLD,
                        response.orderId(),
                        sellPrice,
                        netProfitIfSold
                );

        bets.put(
                bet.id(),
                sold
        );

        openSlugs.remove(slug);

        log.info(
                "REAL SELL FILLED: id={} sellOrderId={} slug={} side={} boughtAt={} soldAt={} sellSize={} winChance={} holdValue={} sellValue={} sellingEv={} threshold={} profitLoss={}",
                bet.id(),
                response.orderId(),
                slug,
                bet.side(),
                bet.price(),
                sellPrice,
                sellSize,
                round(winChance),
                round(holdValue),
                round(sellValue),
                round(sellingEv),
                round(sellThreshold),
                netProfitIfSold
        );

        return sold;
    }

    private BigDecimal netSellProceeds(
            BigDecimal shares,
            BigDecimal price) {

        BigDecimal grossProceeds =
                shares.multiply(price);

        BigDecimal feeRate =
                BigDecimal.valueOf(
                        tradingProperties.takerFee()
                );

        BigDecimal fee =
                shares
                        .multiply(feeRate)
                        .multiply(price)
                        .multiply(
                                BigDecimal.ONE.subtract(price)
                        )
                        .setScale(
                                5,
                                RoundingMode.HALF_UP
                        );

        return grossProceeds.subtract(fee);
    }

    private BigDecimal getExecutablePrice(
            PolymarketMarketSnapshot snapshot,
            MarketSide side) {

        BigDecimal price =
                side == MarketSide.UP
                        ? snapshot.upPrice()
                        : snapshot.downPrice();

        if (price == null) {
            throw new IllegalStateException(
                    "No executable "
                            + side
                            + " price available for "
                            + snapshot.slug()
            );
        }

        return price;
    }

    private void validatePrice(
            BigDecimal price) {

        if (price == null
                || price.signum() <= 0
                || price.compareTo(BigDecimal.ONE) >= 0) {

            throw new IllegalStateException(
                    "Invalid Polymarket price: "
                            + price
            );
        }
    }

    private double round(
            double value) {

        return Math.round(
                value * 10000.0
        ) / 10000.0;
    }

    public Map<String, RealBet> getBets() {
        return Map.copyOf(bets);
    }

    public static class FokNotFilledException
            extends IllegalStateException {

        public FokNotFilledException(
                String message) {

            super(
                    message == null
                            ? "FOK order was not completely filled"
                            : message
            );
        }
    }
}