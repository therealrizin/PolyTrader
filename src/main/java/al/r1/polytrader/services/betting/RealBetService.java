package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.RealBetStatus;
import al.r1.polytrader.services.polymarket.PolymarketMarketResolver;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    private final TradingProperties tradingProperties;
    private final PolymarketMarketResolver marketResolver;
    private final WebClient executionWebClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Set<String> openSlugs =
            ConcurrentHashMap.newKeySet();

    private final Map<String, RealBet> bets =
            new ConcurrentHashMap<>();

    /*
     * Sell at most this fraction of the recorded position size, as a
     * buffer against fill/balance accounting drift not fully captured
     * by using the executor's actual fill amount (see placeRealBet).
     * 0.995 = sell up to 99.5% of the recorded shares.
     */
    private static final BigDecimal SELL_SAFETY_MARGIN =
            new BigDecimal("0.995");

    public RealBetService(
            TradingProperties tradingProperties,
            PolymarketMarketResolver marketResolver,
            @Qualifier("executionWebClient")
            WebClient executionWebClient) {

        this.tradingProperties = tradingProperties;
        this.marketResolver = marketResolver;
        this.executionWebClient = executionWebClient;
    }

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public RealBet placeRealBet(
            PolymarketMarketSnapshot snapshot,
            MarketSide side,
            double countedEv,
            double countedWinChance) {

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Polymarket snapshot cannot be null");
        }

        String slug = snapshot.slug();

        if (slug == null || slug.isBlank()) {
            throw new IllegalArgumentException(
                    "Polymarket market slug cannot be empty");
        }

        if (!openSlugs.add(slug)) {
            throw new IllegalStateException(
                    "Already have an open real bet for market " + slug);
        }

        try {
            BigDecimal price =
                    getExecutablePrice(snapshot, side);

            validatePrice(price);

            BigDecimal amount =
                    tradingProperties.betAmount();

            if (amount == null ||
                    amount.compareTo(BigDecimal.ZERO) <= 0) {

                throw new IllegalStateException(
                        "trading.bet-amount must be > 0");
            }

            PolymarketMarketResolver.ResolvedMarket market =
                    marketResolver.resolveCurrentMarket()
                            .filter(m -> m.slug().equals(slug))
                            .orElseThrow(() ->
                                    new IllegalStateException(
                                            "Could not resolve CLOB token IDs for "
                                                    + slug));

            String tokenId =
                    side == MarketSide.UP
                            ? market.upTokenId()
                            : market.downTokenId();

            if (tokenId == null || tokenId.isBlank()) {
                throw new IllegalStateException(
                        "Resolved token ID is empty for " + slug
                                + " / " + side);
            }

            BigDecimal size =
                    amount.divide(
                            price,
                            8,
                            RoundingMode.DOWN);

            if (size.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalStateException(
                        "Calculated order size is zero");
            }

            BigDecimal amountUsdc =
                    price.multiply(size)
                            .setScale(
                                    2,
                                    RoundingMode.HALF_UP);

            String clientBetId =
                    UUID.randomUUID().toString();

            // For buy orders, we do not set the 'amount' field (null)
            ExecutionOrderRequest request =
                    new ExecutionOrderRequest(
                            clientBetId,
                            slug,
                            tokenId,
                            "BUY",
                            price.toPlainString(),
                            size.toPlainString(),
                            amountUsdc.toPlainString(),
                            "FOK",
                            null   // amount (shares) not needed for buy
                    );

            try {
                String json =
                        objectMapper.writeValueAsString(request);

                log.info(
                        "REAL BET REQUEST JSON: {}",
                        json);

            } catch (Exception e) {
                log.warn(
                        "Failed to serialize request JSON",
                        e);
            }

            log.info(
                    "REAL BET submitting: id={} slug={} side={} tokenId={} price={} size={} amount={} EV={} winChance={} secondsUntilClose={}",
                    clientBetId,
                    slug,
                    side,
                    tokenId,
                    price,
                    size,
                    amount,
                    countedEv,
                    countedWinChance,
                    snapshot.secondsUntilClose()
            );

            ExecutionOrderResponse response;

            try {
                response =
                        executionWebClient.post()
                                .uri("/order")
                                .bodyValue(request)
                                .retrieve()
                                .bodyToMono(
                                        ExecutionOrderResponse.class)
                                .block(
                                        Duration.ofSeconds(10));

            } catch (WebClientResponseException e) {

                String errorBody =
                        e.getResponseBodyAsString();

                log.error(
                        "Executor error response (status {}): {}",
                        e.getStatusCode(),
                        errorBody);

                throw new IllegalStateException(
                        "Executor rejected order: "
                                + errorBody,
                        e);
            }

            if (response == null) {
                throw new IllegalStateException(
                        "Execution service returned empty response");
            }

            if (!response.success()) {
                throw new IllegalStateException(
                        "Polymarket order rejected: "
                                + response.error());
            }

            if (response.orderId() == null ||
                    response.orderId().isBlank()) {

                throw new IllegalStateException(
                        "Polymarket returned success but no order_id");
            }

            Instant placedAt =
                    Instant.now();

            BigDecimal actualSize =
                    (response.takingAmount() != null
                            && response.takingAmount().signum() > 0)
                            ? response.takingAmount()
                            : size;

            if (actualSize.compareTo(size) != 0) {
                log.info(
                        "REAL BET actual fill size differs from estimate: "
                                + "id={} slug={} estimatedSize={} actualFillSize={} "
                                + "(makingAmount={} takingAmount={})",
                        clientBetId, slug, size, actualSize,
                        response.makingAmount(), response.takingAmount()
                );
            }

            RealBet bet =
                    new RealBet(
                            clientBetId,
                            response.orderId(),
                            tokenId,
                            slug,
                            side,
                            amount,
                            price,
                            actualSize,
                            countedEv,
                            countedWinChance,
                            placedAt,
                            snapshot.secondsUntilClose(),
                            RealBetStatus.OPEN,
                            null,
                            null,
                            null
                    );

            bets.put(
                    clientBetId,
                    bet);

            log.info(
                    "REAL BET ACCEPTED: id={} orderId={} slug={} side={} price={} recordedSize={} amount={}",
                    clientBetId,
                    response.orderId(),
                    slug,
                    side,
                    price,
                    actualSize,
                    amount
            );

            return bet;

        } catch (Exception e) {

            openSlugs.remove(slug);

            log.error(
                    "REAL BET FAILED: slug={} side={}",
                    slug,
                    side,
                    e);

            throw e;
        }
    }

    /**
     * Checks whether the currently open REAL bet (if any) for the market
     * in {@code snapshot} should be sold back to the market right now.
     *
     * Same rule as the mock version: win chance is irrelevant here — the
     * outcome of selling is known and certain (the current bid), so the
     * only gate is whether the realized, fee-adjusted EV clears
     * {@code trading.minimum-expected-ev}, the same bar used for entries.
     *
     * On a successful sell the slug is freed from {@code openSlugs}
     * immediately so the trading loop can re-enter the same window on the
     * very next tick if a fresh edge appears.
     */
    public synchronized Optional<RealBet> sellOpenPosition(
            PolymarketMarketSnapshot snapshot
    ) {
        if (snapshot == null) {
            return Optional.empty();
        }

        String slug = snapshot.slug();

        if (slug == null || !openSlugs.contains(slug)) {
            return Optional.empty();
        }

        RealBet bet = findOpenBetForSlug(slug);

        if (bet == null) {
            return Optional.empty();
        }

        BigDecimal currentBid =
                bet.side() == MarketSide.UP
                        ? snapshot.upBid()
                        : snapshot.downBid();

        if (currentBid == null || currentBid.signum() <= 0) {
            log.info(
                    "Cannot evaluate REAL sell for {} on {}: no live bid for side={}",
                    bet.id(), slug, bet.side()
            );
            return Optional.empty();
        }

        BigDecimal netProfitIfSold = netProfitFromSelling(bet, currentBid);

        double sellingEv =
                netProfitIfSold
                        .divide(bet.amount(), 8, RoundingMode.HALF_UP)
                        .doubleValue();

        if (sellingEv < tradingProperties.minimumExpectedEv()) {
            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} sellingEv={} threshold={} -> hold",
                    slug, bet.id(), bet.side(), currentBid, sellingEv,
                    tradingProperties.minimumExpectedEv()
            );
            return Optional.empty();
        }

        try {
            return Optional.of(executeSell(bet, currentBid, netProfitIfSold, sellingEv));
        } catch (Exception e) {
            log.error(
                    "REAL SELL FAILED: id={} slug={} side={}",
                    bet.id(), slug, bet.side(), e
            );
            return Optional.empty();
        }
    }

    private RealBet findOpenBetForSlug(String slug) {
        for (RealBet bet : bets.values()) {
            if (bet.status() == RealBetStatus.OPEN
                    && slug.equals(bet.marketSlug())) {
                return bet;
            }
        }
        return null;
    }

    /**
     * Net profit (fee-adjusted) if the position were closed right now at
     * {@code currentBid}. Fee only applies to positive profit — a losing
     * close is just the shares' mark-to-market loss, not further reduced.
     */
    private BigDecimal netProfitFromSelling(
            RealBet bet,
            BigDecimal currentBid
    ) {
        BigDecimal grossProceeds =
                bet.size().multiply(currentBid);

        BigDecimal grossProfit =
                grossProceeds.subtract(bet.amount());

        if (grossProfit.signum() <= 0) {
            return grossProfit.setScale(4, RoundingMode.HALF_UP);
        }

        return grossProfit
                .multiply(
                        BigDecimal.ONE.subtract(
                                BigDecimal.valueOf(tradingProperties.takerFee())
                        )
                )
                .setScale(4, RoundingMode.HALF_UP);
    }

    private RealBet executeSell(
            RealBet bet,
            BigDecimal currentBid,
            BigDecimal netProfitIfSold,
            double sellingEv
    ) {
        String slug = bet.marketSlug();

        BigDecimal safeHeldSize =
                bet.size().multiply(SELL_SAFETY_MARGIN);

        BigDecimal sellSize =
                safeHeldSize.setScale(2, RoundingMode.DOWN);

        if (sellSize.signum() <= 0) {
            throw new IllegalStateException(
                    "Position size " + bet.size()
                            + " rounds down to zero sellable shares (2 decimal limit)");
        }

        BigDecimal amountUsdc =
                sellSize.multiply(currentBid)
                        .setScale(2, RoundingMode.HALF_UP);

        String clientOrderId = UUID.randomUUID().toString();

        ExecutionOrderRequest request =
                new ExecutionOrderRequest(
                        clientOrderId,
                        slug,
                        bet.tokenId(),
                        "SELL",
                        currentBid.toPlainString(),
                        sellSize.toPlainString(),     // size in shares (2dp)
                        amountUsdc.toPlainString(),   // estimated USDC proceeds
                        "FOK",
                        sellSize.toPlainString()      // amount = shares (2dp, fixes the error)
                );

        try {
            String json = objectMapper.writeValueAsString(request);
            log.info("REAL SELL REQUEST JSON: {}", json);
        } catch (Exception e) {
            log.warn("Failed to serialize sell request JSON", e);
        }

        log.info(
                "REAL SELL submitting: id={} boughtOrderId={} slug={} side={} tokenId={} " +
                        "bid={} recordedSize={} safeSize={} sellSize={} originalAmount={} " +
                        "netProfitIfSold={} sellingEv={} threshold={}",
                clientOrderId, bet.orderId(), slug, bet.side(), bet.tokenId(),
                currentBid, bet.size(), safeHeldSize, sellSize, bet.amount(),
                netProfitIfSold, sellingEv,
                tradingProperties.minimumExpectedEv()
        );

        ExecutionOrderResponse response;

        try {
            response =
                    executionWebClient.post()
                            .uri("/order")
                            .bodyValue(request)
                            .retrieve()
                            .bodyToMono(ExecutionOrderResponse.class)
                            .block(Duration.ofSeconds(10));

        } catch (WebClientResponseException e) {

            String errorBody = e.getResponseBodyAsString();

            log.error(
                    "REAL SELL executor error response (status {}): {}",
                    e.getStatusCode(), errorBody
            );

            throw new IllegalStateException(
                    "Executor rejected sell order: " + errorBody, e);
        }

        if (response == null) {
            throw new IllegalStateException(
                    "Execution service returned empty response for sell");
        }

        if (!response.success()) {
            throw new IllegalStateException(
                    "Polymarket sell order rejected: " + response.error());
        }

        if (response.orderId() == null || response.orderId().isBlank()) {
            throw new IllegalStateException(
                    "Polymarket sell returned success but no order_id");
        }

        RealBet sold = new RealBet(
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
                RealBetStatus.SOLD,
                response.orderId(),
                currentBid,
                netProfitIfSold
        );

        bets.put(bet.id(), sold);

        openSlugs.remove(slug);

        log.info(
                "REAL SELL ACCEPTED: id={} sellOrderId={} slug={} side={} boughtAt={} soldAt={} " +
                        "netProfit={} sellingEv={} (slot freed for re-entry)",
                bet.id(), response.orderId(), slug, bet.side(),
                bet.price(), currentBid, netProfitIfSold, sellingEv
        );

        return sold;
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
                            + snapshot.slug());
        }

        return price;
    }

    private void validatePrice(BigDecimal price) {

        if (price.compareTo(BigDecimal.ZERO) <= 0 ||
                price.compareTo(BigDecimal.ONE) >= 0) {

            throw new IllegalStateException(
                    "Invalid Polymarket price: "
                            + price);
        }
    }

    public Map<String, RealBet> getBets() {
        return Map.copyOf(bets);
    }

    public record ExecutionOrderRequest(

            @JsonProperty("client_order_id")
            String clientOrderId,

            @JsonProperty("market_slug")
            String marketSlug,

            @JsonProperty("token_id")
            String tokenId,

            String side,

            String price,

            String size,

            @JsonProperty("amount_usdc")
            String amountUsdc,

            @JsonProperty("order_type")
            String orderType,

            @JsonProperty("amount")
            String amount
    ) {
    }

    public record ExecutionOrderResponse(

            boolean success,

            @JsonProperty("order_id")
            String orderId,

            String status,

            String error,

            @JsonProperty("making_amount")
            BigDecimal makingAmount,

            @JsonProperty("taking_amount")
            BigDecimal takingAmount
    ) {
    }

    public record RealBet(

            String id,

            @JsonProperty("order_id")
            String orderId,

            @JsonProperty("token_id")
            String tokenId,

            @JsonProperty("market_slug")
            String marketSlug,

            MarketSide side,

            BigDecimal amount,

            BigDecimal price,

            BigDecimal size,

            @JsonProperty("counted_ev")
            double countedEv,

            @JsonProperty("counted_win_chance")
            double countedWinChance,

            @JsonProperty("placed_at")
            Instant placedAt,

            @JsonProperty("seconds_until_close")
            long secondsUntilClose,

            RealBetStatus status,

            @JsonProperty("sell_order_id")
            String sellOrderId,

            @JsonProperty("sold_price")
            BigDecimal soldPrice,

            @JsonProperty("profit_loss")
            BigDecimal profitLoss
    ) {
    }
}