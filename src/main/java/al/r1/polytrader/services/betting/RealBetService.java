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

    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;
    private static final BigDecimal SELL_SAFETY_MARGIN = new BigDecimal("0.995");

    private final TradingProperties tradingProperties;
    private final PolymarketMarketResolver marketResolver;
    private final WebClient executionWebClient;
    private final Prices prices;
    private final TradingEngine tradingEngine;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();
    private final Map<String, RealBet> bets = new ConcurrentHashMap<>();

    public RealBetService(
            TradingProperties tradingProperties,
            PolymarketMarketResolver marketResolver,
            @Qualifier("executionWebClient") WebClient executionWebClient,
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

    public RealBet placeRealBet(PolymarketMarketSnapshot snapshot, MarketSide side, double countedEv, double countedWinChance) {
        if (snapshot == null) {
            throw new IllegalArgumentException("Polymarket snapshot cannot be null");
        }

        String slug = snapshot.slug();
        if (slug == null || slug.isBlank()) {
            throw new IllegalArgumentException("Polymarket market slug cannot be empty");
        }

        if (!openSlugs.add(slug)) {
            throw new IllegalStateException("Already have an open real bet for market " + slug);
        }

        try {
            requireFreshPrice(slug);

            BigDecimal price = getExecutablePrice(snapshot, side);
            validatePrice(price);

            BigDecimal amount = tradingProperties.betAmount();
            if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalStateException("trading.bet-amount must be > 0");
            }

            PolymarketMarketResolver.ResolvedMarket market =
                    marketResolver.resolveCurrentMarket()
                            .filter(m -> m.slug().equals(slug))
                            .orElseThrow(() -> new IllegalStateException("Could not resolve CLOB token IDs for " + slug));

            String tokenId = side == MarketSide.UP ? market.upTokenId() : market.downTokenId();
            if (tokenId == null || tokenId.isBlank()) {
                throw new IllegalStateException("Resolved token ID is empty for " + slug + " / " + side);
            }

            requireFreshPrice(slug);

            BigDecimal size = amount.divide(price, 8, RoundingMode.DOWN);
            if (size.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalStateException("Calculated order size is zero");
            }

            BigDecimal amountUsdc = price.multiply(size).setScale(2, RoundingMode.HALF_UP);
            String clientBetId = UUID.randomUUID().toString();

            ExecutionOrderRequest request = new ExecutionOrderRequest(
                    clientBetId,
                    slug,
                    tokenId,
                    "BUY",
                    price.toPlainString(),
                    size.toPlainString(),
                    amountUsdc.toPlainString(),
                    "FOK",
                    null
            );

            try {
                String json = objectMapper.writeValueAsString(request);
                log.info("REAL BET REQUEST JSON: {}", json);
            } catch (Exception e) {
                log.warn("Failed to serialize request JSON", e);
            }

            requireFreshPrice(slug);

            log.info(
                    "REAL BET submitting: id={} slug={} side={} tokenId={} price={} size={} amount={} EV={} winChance={} secondsUntilClose={} priceAgeMs={}",
                    clientBetId,
                    slug,
                    side,
                    tokenId,
                    price,
                    size,
                    amount,
                    countedEv,
                    countedWinChance,
                    snapshot.secondsUntilClose(),
                    prices.getPriceAgeMillis(SYMBOL)
            );

            ExecutionOrderResponse response;
            try {
                response = executionWebClient.post()
                        .uri("/order")
                        .bodyValue(request)
                        .retrieve()
                        .bodyToMono(ExecutionOrderResponse.class)
                        .block(Duration.ofSeconds(10));
            } catch (WebClientResponseException e) {
                String errorBody = e.getResponseBodyAsString();
                log.error("Executor error response (status {}): {}", e.getStatusCode(), errorBody);
                throw new IllegalStateException("Executor rejected order: " + errorBody, e);
            }

            if (response == null) {
                throw new IllegalStateException("Execution service returned empty response");
            }
            if (!response.success()) {
                throw new IllegalStateException("Polymarket order rejected: " + response.error());
            }
            if (response.orderId() == null || response.orderId().isBlank()) {
                throw new IllegalStateException("Polymarket returned success but no order_id");
            }

            Instant placedAt = Instant.now();
            BigDecimal actualSize = response.takingAmount() != null && response.takingAmount().signum() > 0
                    ? response.takingAmount()
                    : size;

            if (actualSize.compareTo(size) != 0) {
                log.info(
                        "REAL BET actual fill size differs from estimate: id={} slug={} estimatedSize={} actualFillSize={} (makingAmount={} takingAmount={})",
                        clientBetId,
                        slug,
                        size,
                        actualSize,
                        response.makingAmount(),
                        response.takingAmount()
                );
            }

            RealBet bet = new RealBet(
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
                    BetStatus.OPEN,
                    null,
                    null,
                    null
            );

            bets.put(clientBetId, bet);

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
            log.error("REAL BET FAILED: slug={} side={}", slug, side, e);
            throw e;
        }
    }

    private void requireFreshPrice(String slug) {
        long observedAtMillis = prices.getLastPriceTimestampMillis(SYMBOL);
        long ageMillis = prices.getPriceAgeMillis(SYMBOL);
        BigDecimal currentPrice = prices.getPrice(SYMBOL);

        if (observedAtMillis <= 0) {
            log.warn(
                    "REAL BET BLOCKED: slug={} reason=NO_RTDS_PRICE symbol={} currentPrice={} lastTimestamp={}",
                    slug,
                    SYMBOL,
                    currentPrice,
                    observedAtMillis
            );
            throw new IllegalStateException("Cannot place real bet: no timestamped Polymarket RTDS Chainlink price available");
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
                    "Cannot place real bet: Chainlink RTDS price is stale. Age="
                            + ageMillis
                            + "ms, maximum="
                            + Prices.MAX_PRICE_AGE_MILLIS
                            + "ms"
            );
        }
    }

    public synchronized Optional<RealBet> sellOpenPosition(PolymarketMarketSnapshot snapshot) {
        if (snapshot == null) {
            return Optional.empty();
        }

        if (!prices.isPriceFresh(SYMBOL)) {
            log.info(
                    "SELL_CHECK (REAL) slug={} -> hold (Chainlink price stale, ageMs={})",
                    snapshot.slug(),
                    prices.getPriceAgeMillis(SYMBOL)
            );
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

        BigDecimal currentBid = bet.side() == MarketSide.UP
                ? snapshot.upBid()
                : snapshot.downBid();

        if (currentBid == null || currentBid.signum() <= 0) {
            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} -> hold (no live bid)",
                    slug,
                    bet.id(),
                    bet.side(),
                    currentBid
            );
            return Optional.empty();
        }

        BigDecimal currentLivePrice = prices.getPrice(SYMBOL);
        BigDecimal currentTwapPrice = prices.getAvg60sPrice(SYMBOL);

        if (currentLivePrice == null
                || currentLivePrice.signum() <= 0
                || currentTwapPrice == null
                || currentTwapPrice.signum() <= 0
                || snapshot.strikePriceUsd() == null) {

            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} -> hold (price model unavailable)",
                    slug,
                    bet.id(),
                    bet.side(),
                    currentBid
            );
            return Optional.empty();
        }

        var estimate = tradingEngine.estimateUpDown(
                currentLivePrice,
                currentTwapPrice,
                snapshot.strikePriceUsd(),
                (int) snapshot.secondsUntilClose(),
                snapshot.upPrice() != null ? snapshot.upPrice().doubleValue() : 0.5,
                snapshot.downPrice() != null ? snapshot.downPrice().doubleValue() : 0.5,
                tradingProperties.takerFee()
        );

        double winChance = bet.side() == MarketSide.UP ? estimate.upChance() : estimate.downChance();
        double holdValue = tradingEngine.holdValuePerShare(winChance);
        double sellValue = tradingEngine.netSellValuePerShare(currentBid.doubleValue(), tradingProperties.takerFee());
        double sellingEv = sellValue - holdValue;
        double currentReturn = bet.price().signum() > 0
                ? currentBid.divide(bet.price(), 8, RoundingMode.HALF_UP).doubleValue() - 1.0
                : 0.0;
        double sellThreshold = tradingProperties.minimumExpectedEv() / 3.0;

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
            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} currentBid={} winChance={} holdValue={} sellValue={} sellingEv={} threshold={} -> hold",
                    slug,
                    bet.id(),
                    bet.side(),
                    currentBid,
                    round(winChance),
                    round(holdValue),
                    round(sellValue),
                    round(sellingEv),
                    round(sellThreshold)
            );
            return Optional.empty();
        }

        if (!prices.isPriceFresh(SYMBOL)) {
            log.info(
                    "SELL_CHECK (REAL) slug={} betId={} side={} -> hold (Chainlink price became stale before sell, ageMs={})",
                    slug,
                    bet.id(),
                    bet.side(),
                    prices.getPriceAgeMillis(SYMBOL)
            );
            return Optional.empty();
        }

        try {
            return Optional.of(executeSell(bet, currentBid, sellingEv, winChance, holdValue, sellValue, sellThreshold));
        } catch (Exception e) {
            log.error("REAL SELL FAILED: id={} slug={} side={}", bet.id(), slug, bet.side(), e);
            return Optional.empty();
        }
    }

    private RealBet findOpenBetForSlug(String slug) {
        for (RealBet bet : bets.values()) {
            if (bet.status() == BetStatus.OPEN && slug.equals(bet.marketSlug())) {
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

        String slug = bet.marketSlug();
        BigDecimal safeHeldSize = bet.size().multiply(SELL_SAFETY_MARGIN);
        BigDecimal sellSize = safeHeldSize.setScale(2, RoundingMode.DOWN);

        if (sellSize.signum() <= 0) {
            throw new IllegalStateException("Position size " + bet.size() + " rounds down to zero sellable shares");
        }

        BigDecimal amountUsdc = sellSize.multiply(currentBid).setScale(2, RoundingMode.HALF_UP);
        String clientOrderId = UUID.randomUUID().toString();

        ExecutionOrderRequest request = new ExecutionOrderRequest(
                clientOrderId,
                slug,
                bet.tokenId(),
                "SELL",
                currentBid.toPlainString(),
                sellSize.toPlainString(),
                amountUsdc.toPlainString(),
                "FOK",
                sellSize.toPlainString()
        );

        try {
            String json = objectMapper.writeValueAsString(request);
            log.info("REAL SELL REQUEST JSON: {}", json);
        } catch (Exception e) {
            log.warn("Failed to serialize sell request JSON", e);
        }

        BigDecimal netProceeds = netSellProceeds(sellSize, currentBid);
        BigDecimal netProfitIfSold = netProceeds.subtract(bet.amount());

        log.info(
                "REAL SELL submitting: id={} boughtOrderId={} slug={} side={} tokenId={} bid={} recordedSize={} safeSize={} sellSize={} originalAmount={} winChance={} holdValue={} sellValue={} sellingEv={} threshold={} netProceeds={} profitIfSold={}",
                clientOrderId,
                bet.orderId(),
                slug,
                bet.side(),
                bet.tokenId(),
                currentBid,
                bet.size(),
                safeHeldSize,
                sellSize,
                bet.amount(),
                round(winChance),
                round(holdValue),
                round(sellValue),
                round(sellingEv),
                round(sellThreshold),
                netProceeds,
                netProfitIfSold
        );

        ExecutionOrderResponse response;
        try {
            response = executionWebClient.post()
                    .uri("/order")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(ExecutionOrderResponse.class)
                    .block(Duration.ofSeconds(10));
        } catch (WebClientResponseException e) {
            String errorBody = e.getResponseBodyAsString();
            log.error("REAL SELL executor error response (status {}): {}", e.getStatusCode(), errorBody);
            throw new IllegalStateException("Executor rejected sell order: " + errorBody, e);
        }

        if (response == null) {
            throw new IllegalStateException("Execution service returned empty response for sell");
        }
        if (!response.success()) {
            throw new IllegalStateException("Polymarket sell order rejected: " + response.error());
        }
        if (response.orderId() == null || response.orderId().isBlank()) {
            throw new IllegalStateException("Polymarket sell returned success but no order_id");
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
                BetStatus.SOLD,
                response.orderId(),
                currentBid,
                netProfitIfSold
        );

        bets.put(bet.id(), sold);
        openSlugs.remove(slug);

        log.info(
                "REAL SELL ACCEPTED: id={} sellOrderId={} slug={} side={} boughtAt={} soldAt={} winChance={} holdValue={} sellValue={} sellingEv={} threshold={} profitLoss={} (slot freed for re-entry)",
                bet.id(),
                response.orderId(),
                slug,
                bet.side(),
                bet.price(),
                currentBid,
                round(winChance),
                round(holdValue),
                round(sellValue),
                round(sellingEv),
                round(sellThreshold),
                netProfitIfSold
        );

        return sold;
    }

    private BigDecimal netSellProceeds(BigDecimal shares, BigDecimal price) {
        BigDecimal grossProceeds = shares.multiply(price);
        BigDecimal feeRate = BigDecimal.valueOf(tradingProperties.takerFee());
        BigDecimal fee = shares
                .multiply(feeRate)
                .multiply(price)
                .multiply(BigDecimal.ONE.subtract(price))
                .setScale(5, RoundingMode.HALF_UP);
        return grossProceeds.subtract(fee);
    }

    private BigDecimal getExecutablePrice(PolymarketMarketSnapshot snapshot, MarketSide side) {
        BigDecimal price = side == MarketSide.UP ? snapshot.upPrice() : snapshot.downPrice();
        if (price == null) {
            throw new IllegalStateException("No executable " + side + " price available for " + snapshot.slug());
        }
        return price;
    }

    private void validatePrice(BigDecimal price) {
        if (price.compareTo(BigDecimal.ZERO) <= 0 || price.compareTo(BigDecimal.ONE) >= 0) {
            throw new IllegalStateException("Invalid Polymarket price: " + price);
        }
    }

    private double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    public Map<String, RealBet> getBets() {
        return Map.copyOf(bets);
    }
}