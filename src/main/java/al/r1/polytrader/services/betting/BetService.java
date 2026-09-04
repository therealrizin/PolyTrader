package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.ExecutionOrderRequest;
import al.r1.polytrader.services.betting.model.ExecutionOrderResponse;
import al.r1.polytrader.services.betting.model.Bet;
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
public class BetService {

    private static final BigDecimal SELL_SAFETY_MARGIN = new BigDecimal("0.995");
    private static final int PRICE_SCALE = 2;
    private static final int BUY_SIZE_SCALE = 2;
    private static final int USDC_SCALE = 2;
    private static final BigDecimal MIN_ORDER_USDC = new BigDecimal("1.00");

    private final TradingProperties tradingProperties;
    private final PolymarketMarketResolver marketResolver;
    private final WebClient executionWebClient;
    private final Prices prices;
    private final TradingEngine tradingEngine;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();
    private final Map<String, Bet> bets = new ConcurrentHashMap<>();

    public BetService(TradingProperties tradingProperties, PolymarketMarketResolver marketResolver,
                      @Qualifier("executionWebClient") WebClient executionWebClient, Prices prices, TradingEngine tradingEngine) {
        this.tradingProperties = tradingProperties;
        this.marketResolver = marketResolver;
        this.executionWebClient = executionWebClient;
        this.prices = prices;
        this.tradingEngine = tradingEngine;
    }

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public Optional<Bet> getOpenBetFor(String marketSlug) {
        return Optional.ofNullable(findOpenBetForSlug(marketSlug));
    }

    public Bet placeBet(PolymarketMarketSnapshot snapshot, MarketSide side, Double maxBetPrice, double countedEv,
                        double countedWinChance, ChainlinkSymbol symbol) {
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
            requireFreshPrice(slug, symbol);
            validatePrice(maxBetPrice);
            BigDecimal tickSafePrice = BigDecimal.valueOf(maxBetPrice).setScale(PRICE_SCALE, RoundingMode.DOWN);
            if (tickSafePrice.signum() <= 0) {
                throw new FokNotFilledException("Max acceptable price " + maxBetPrice + " rounds to zero");
            }

            BigDecimal configuredAmount = tradingProperties.betAmount();
            if (configuredAmount == null || configuredAmount.signum() <= 0) {
                throw new IllegalStateException("trading.bet-amount must be > 0");
            }
            if (configuredAmount.compareTo(MIN_ORDER_USDC) < 0) {
                throw new IllegalStateException("Configured bet amount " + configuredAmount + " is below Polymarket minimum " + MIN_ORDER_USDC);
            }

            BigDecimal amountUsdc = configuredAmount.setScale(USDC_SCALE, RoundingMode.DOWN);
            if (amountUsdc.compareTo(MIN_ORDER_USDC) < 0) {
                throw new IllegalStateException("Configured bet amount becomes " + amountUsdc + " after cent rounding");
            }
            assertMaxDecimals(amountUsdc, USDC_SCALE, "BUY amountUsdc");

            BigDecimal estimatedSize = amountUsdc.divide(tickSafePrice, BUY_SIZE_SCALE, RoundingMode.DOWN);
            if (estimatedSize.signum() <= 0) {
                throw new IllegalStateException("Calculated BUY size is zero: amount=" + amountUsdc + ", price=" + tickSafePrice);
            }
            assertMaxDecimals(estimatedSize, BUY_SIZE_SCALE, "BUY estimatedSize");

            String clientBetId = UUID.randomUUID().toString();
            PolymarketMarketResolver.ResolvedMarket market = marketResolver.resolveCurrentMarket()
                    .filter(m -> slug.equals(m.slug()))
                    .orElseThrow(() -> new IllegalStateException("Could not resolve CLOB token IDs for " + slug));

            String tokenId = side == MarketSide.UP ? market.upTokenId() : market.downTokenId();
            if (tokenId == null || tokenId.isBlank()) {
                throw new IllegalStateException("Resolved token ID is empty for " + slug + " / " + side);
            }

            requireFreshPrice(slug, symbol);

            ExecutionOrderRequest request = new ExecutionOrderRequest(clientBetId, slug, tokenId, "BUY",
                    tickSafePrice.toPlainString(), estimatedSize.toPlainString(), amountUsdc.toPlainString(), "FOK", null);

            logBuyRequest(request, slug, side, tickSafePrice, estimatedSize, amountUsdc, configuredAmount, countedEv, countedWinChance, snapshot, symbol);

            ExecutionOrderResponse response;
            try {
                response = executionWebClient.post().uri("/order").bodyValue(request).retrieve()
                        .bodyToMono(ExecutionOrderResponse.class).block(Duration.ofSeconds(10));
            } catch (WebClientResponseException e) {
                String errorBody = e.getResponseBodyAsString();
                log.error("Executor BUY rejected: status={} body={} requestPrice={} requestAmountUsdc={} requestSize={}",
                        e.getStatusCode(), errorBody, tickSafePrice, amountUsdc, estimatedSize);
                throw new IllegalStateException("Executor rejected BUY order: " + errorBody, e);
            }

            if (response == null) {
                throw new IllegalStateException("Execution service returned empty BUY response");
            }
            if (!response.success()) {
                log.info("REAL FOK BUY NOT FILLED: id={} slug={} side={} price={} amountUsdc={} estimatedSize={} error={}",
                        clientBetId, slug, side, tickSafePrice, amountUsdc, estimatedSize, response.error());
                throw new FokNotFilledException(response.error());
            }
            if (response.orderId() == null || response.orderId().isBlank()) {
                throw new IllegalStateException("BUY succeeded but no order_id returned");
            }

            BigDecimal actualSize = response.takingAmount() != null && response.takingAmount().signum() > 0 ? response.takingAmount() : estimatedSize;
            if (actualSize.signum() <= 0) {
                throw new FokNotFilledException("FOK returned zero filled size");
            }

            Instant placedAt = Instant.now();
            Bet bet = new Bet(clientBetId, response.orderId(), tokenId, slug, side, amountUsdc, tickSafePrice, actualSize,
                    countedEv, countedWinChance, placedAt, snapshot.secondsUntilClose(), BetStatus.OPEN, null, null, null);
            bets.put(clientBetId, bet);

            log.info("BET_DECISION mode=REAL action=FILLED slug={} side={} | winChance={} evAtTradePrice={} | tradePrice={} amountUsdc={} filledShares={} | secondsLeft={}",
                    slug, side, round(countedWinChance), round(countedEv), tickSafePrice, amountUsdc, actualSize, snapshot.secondsUntilClose());

            return bet;
        } catch (Exception e) {
            openSlugs.remove(slug);
            if (e instanceof FokNotFilledException) {
                log.info("REAL FOK BUY DID NOT FILL: slug={} side={} reason={}", slug, side, e.getMessage());
            } else {
                log.error("REAL FOK BUY FAILED: slug={} side={}", slug, side, e);
            }
            throw e;
        }
    }

    public void sellPositionWithLimit(PolymarketMarketSnapshot snapshot, MarketSide side, double price,
                                      double ev, double winChance, ChainlinkSymbol symbol) throws FokNotFilledException {
        String slug = snapshot.slug();
        Bet bet = findOpenBetForSlug(slug);
        if (bet == null) {
            throw new IllegalStateException("No open bet found for market " + slug);
        }
        if (bet.side() != side) {
            throw new IllegalStateException("Open bet side " + bet.side() + " does not match requested side " + side);
        }
        requireFreshPrice(slug, symbol);
        if (price <= 0 || price >= 1) {
            throw new IllegalStateException("Sell price out of bounds: " + price);
        }

        BigDecimal sellPrice = BigDecimal.valueOf(price).setScale(PRICE_SCALE, RoundingMode.UP);
        if (sellPrice.signum() <= 0 || sellPrice.compareTo(BigDecimal.ONE) >= 0) {
            throw new IllegalStateException("Invalid SELL price after rounding: " + sellPrice);
        }

        doExecuteSell(bet, sellPrice, winChance, ev);
    }

    public synchronized Optional<Bet> sellOpenPosition(PolymarketMarketSnapshot snapshot, ChainlinkSymbol symbol) {
        if (snapshot == null) {
            return Optional.empty();
        }
        String slug = snapshot.slug();
        if (slug == null || !openSlugs.contains(slug)) {
            return Optional.empty();
        }
        if (!prices.isPriceFresh(symbol)) {
            log.info("SELL_CHECK (REAL) slug={} -> hold (Chainlink stale, ageMs={})", slug, prices.getPriceAgeMillis(symbol));
            return Optional.empty();
        }

        Bet bet = findOpenBetForSlug(slug);
        if (bet == null) {
            return Optional.empty();
        }

        BigDecimal currentLivePrice = prices.getPrice(symbol);
        BigDecimal currentTwapPrice = prices.getAvg60sPrice(symbol);
        BigDecimal strike = snapshot.resolutionPrice();

        if (currentLivePrice == null || currentLivePrice.signum() <= 0
                || currentTwapPrice == null || currentTwapPrice.signum() <= 0
                || strike == null || strike.signum() <= 0) {
            log.info("SELL_CHECK (REAL) slug={} betId={} side={} -> hold (price model unavailable)", slug, bet.id(), bet.side());
            return Optional.empty();
        }

        var estimate = tradingEngine.estimatePricesToMeetEv(currentLivePrice, currentTwapPrice, strike, (int) Math.max(0, snapshot.secondsUntilClose()));
        double winChance = bet.side() == MarketSide.UP ? estimate.upChance() : estimate.downChance();

        if (winChance <= 0.0) {
            log.info("SELL_CHECK (REAL) slug={} betId={} side={} -> hold (invalid winChance)", slug, bet.id(), bet.side());
            return Optional.empty();
        }

        double sellThreshold = tradingEngine.requiredEv(winChance);
        double minSellPrice = tradingEngine.minSellPriceForEv(winChance, sellThreshold);

        BigDecimal currentBid = bet.side() == MarketSide.UP ? snapshot.upBid() : snapshot.downBid();
        double sellingEv = currentBid != null && currentBid.signum() > 0
                ? tradingEngine.netSellValuePerShare(currentBid.doubleValue()) / winChance - 1.0
                : Double.NaN;

        log.info("SELL_CHECK (REAL) slug={} betId={} side={} winChance={} minSellPrice={} threshold={} currentBid={} sellingEv={} btcLivePrice={} btcPriceToAchieve={} secondsLeft={}",
                slug, bet.id(), bet.side(), round(winChance), round(minSellPrice), round(sellThreshold), currentBid, round(sellingEv), currentLivePrice, strike, snapshot.secondsUntilClose());

        try {
            return Optional.of(executeSell(bet, minSellPrice, winChance, sellThreshold));
        } catch (FokNotFilledException e) {
            log.info("REAL FOK SELL NOT FILLED: slug={} betId={} side={} price={} reason={}", slug, bet.id(), bet.side(), round(minSellPrice), e.getMessage());
            return Optional.empty();
        } catch (Exception e) {
            log.error("REAL SELL FAILED: id={} slug={} side={}", bet.id(), slug, bet.side(), e);
            return Optional.empty();
        }
    }

    // --- Internal helpers ---

    private Bet findOpenBetForSlug(String slug) {
        for (Bet bet : bets.values()) {
            if (bet.status() == BetStatus.OPEN && slug.equals(bet.marketSlug())) {
                return bet;
            }
        }
        return null;
    }

    private Bet doExecuteSell(Bet bet, BigDecimal sellPrice, double winChance, double evThreshold) {
        String slug = bet.marketSlug();

        BigDecimal sellSize = bet.size().multiply(SELL_SAFETY_MARGIN).setScale(4, RoundingMode.DOWN);
        if (sellSize.signum() <= 0) {
            throw new IllegalStateException("Position size " + bet.size() + " rounds down to zero sellable shares");
        }
        assertMaxDecimals(sellSize, 4, "SELL sellSize");

        BigDecimal amountUsdc = sellSize.multiply(sellPrice).setScale(USDC_SCALE, RoundingMode.DOWN);
        assertMaxDecimals(amountUsdc, USDC_SCALE, "SELL amountUsdc");

        String clientOrderId = UUID.randomUUID().toString();
        ExecutionOrderRequest request = new ExecutionOrderRequest(clientOrderId, slug, bet.tokenId(), "SELL",
                sellPrice.toPlainString(), sellSize.toPlainString(), amountUsdc.toPlainString(), "FOK", sellSize.toPlainString());

        logSellRequest(request, winChance, evThreshold, sellPrice, sellSize);

        ExecutionOrderResponse response;
        try {
            response = executionWebClient.post().uri("/order").bodyValue(request).retrieve()
                    .bodyToMono(ExecutionOrderResponse.class).block(Duration.ofSeconds(10));
        } catch (WebClientResponseException e) {
            String errorBody = e.getResponseBodyAsString();
            log.error("Executor SELL rejected: status={} body={} requestPrice={} requestSize={}", e.getStatusCode(), errorBody, sellPrice, sellSize);
            throw new IllegalStateException("Executor rejected SELL order: " + errorBody, e);
        }

        if (response == null) {
            throw new IllegalStateException("Execution service returned empty SELL response");
        }
        if (!response.success()) {
            throw new FokNotFilledException(response.error());
        }
        if (response.orderId() == null || response.orderId().isBlank()) {
            throw new IllegalStateException("SELL succeeded but no order_id returned");
        }

        BigDecimal actualSize = response.takingAmount() != null && response.takingAmount().signum() > 0 ? response.takingAmount() : sellSize;
        BigDecimal netProceeds = netSellProceeds(actualSize, sellPrice);
        BigDecimal netProfitIfSold = netProceeds.subtract(bet.amount());

        Bet sold = new Bet(bet.id(), bet.orderId(), bet.tokenId(), slug, bet.side(), bet.amount(), bet.price(), bet.size(),
                bet.countedEv(), bet.countedWinChance(), bet.placedAt(), bet.secondsUntilClose(), BetStatus.SOLD,
                response.orderId(), sellPrice, netProfitIfSold);

        bets.put(bet.id(), sold);
        openSlugs.remove(slug);

        log.info("BET_DECISION mode=REAL action=SOLD slug={} side={} | winChance={} threshold={} | boughtAt={} soldAt={} | sellSize={} profitLoss={}",
                slug, bet.side(), round(winChance), round(evThreshold), bet.price(), sellPrice, actualSize, netProfitIfSold);

        return sold;
    }

    private Bet executeSell(Bet bet, double minSellPrice, double winChance, double sellThreshold) {
        BigDecimal sellPrice = BigDecimal.valueOf(minSellPrice).setScale(PRICE_SCALE, RoundingMode.UP);
        if (sellPrice.signum() <= 0 || sellPrice.compareTo(BigDecimal.ONE) >= 0) {
            throw new IllegalStateException("Invalid SELL price after rounding: " + sellPrice);
        }
        return doExecuteSell(bet, sellPrice, winChance, sellThreshold);
    }

    private void logBuyRequest(ExecutionOrderRequest request, String slug, MarketSide side, BigDecimal price, BigDecimal size,
                               BigDecimal amountUsdc, BigDecimal configuredAmount, double countedEv, double countedWinChance,
                               PolymarketMarketSnapshot snapshot, ChainlinkSymbol symbol) {
        try {
            log.info("REAL FOK BUY REQUEST JSON: {}", objectMapper.writeValueAsString(request));
        } catch (Exception e) {
            log.warn("Failed to serialize BUY request JSON", e);
        }

        BigDecimal currentLivePrice = prices.getPrice(symbol);
        log.info("BET_DECISION mode=REAL action=SUBMITTING slug={} side={} tokenId={} | winChance={} evAtTradePrice={} | btcLivePrice={} btcPriceToAchieve={} | tradePrice={} estimatedSize={} amountUsdc={} configuredAmount={} | secondsUntilClose={} priceAgeMs={}",
                slug, side, request.tokenId(), round(countedWinChance), round(countedEv), currentLivePrice,
                snapshot.resolutionPrice(), price, size, amountUsdc, configuredAmount, snapshot.secondsUntilClose(), prices.getPriceAgeMillis(symbol));
    }

    private void logSellRequest(ExecutionOrderRequest request, double winChance, double sellThreshold, BigDecimal price, BigDecimal size) {
        try {
            log.info("REAL FOK SELL REQUEST JSON: {}", objectMapper.writeValueAsString(request));
        } catch (Exception e) {
            log.warn("Failed to serialize SELL request JSON", e);
        }
        log.info("BET_DECISION mode=REAL action=SUBMITTING_SELL slug={} tokenId={} | winChance={} threshold={} | price={} size={}",
                request.marketSlug(), request.tokenId(), round(winChance), round(sellThreshold), price, size);
    }

    private void requireFreshPrice(String slug, ChainlinkSymbol symbol) {
        long observedAtMillis = prices.getLastPriceTimestampMillis(symbol);
        long ageMillis = prices.getPriceAgeMillis(symbol);
        BigDecimal currentPrice = prices.getPrice(symbol);

        if (observedAtMillis <= 0) {
            log.warn("BET BLOCKED: slug={} reason=NO_RTDS_PRICE symbol={} currentPrice={} lastTimestamp={}", slug, symbol, currentPrice, observedAtMillis);
            throw new IllegalStateException("Cannot place real bet: no timestamped Polymarket RTDS Chainlink price available");
        }
        if (!prices.isPriceFresh(symbol)) {
            log.warn("REAL BET BLOCKED: slug={} reason=STALE_RTDS_PRICE symbol={} currentPrice={} ageMs={} maxAgeMs={} observedAt={} now={}",
                    slug, symbol, currentPrice, ageMillis, Prices.MAX_PRICE_AGE_MILLIS, Instant.ofEpochMilli(observedAtMillis), Instant.now());
            throw new IllegalStateException("Cannot place real bet: Chainlink RTDS price is stale. Age=" + ageMillis + "ms, maximum=" + Prices.MAX_PRICE_AGE_MILLIS + "ms");
        }
    }

    private BigDecimal netSellProceeds(BigDecimal shares, BigDecimal price) {
        BigDecimal grossProceeds = shares.multiply(price);
        BigDecimal feeRate = BigDecimal.valueOf(tradingProperties.takerFee());
        BigDecimal fee = shares.multiply(feeRate).multiply(price).multiply(BigDecimal.ONE.subtract(price)).setScale(5, RoundingMode.HALF_UP);
        return grossProceeds.subtract(fee);
    }

    private void validatePrice(Double price) {
        if (price == null || price <= 0 || price >= 1) {
            throw new IllegalStateException("Invalid Polymarket price: " + price);
        }
    }

    private void assertMaxDecimals(BigDecimal value, int maxScale, String fieldName) {
        int actualScale = value.stripTrailingZeros().scale();
        if (actualScale > maxScale) {
            throw new IllegalStateException(fieldName + " has more than " + maxScale + " decimal places: " + value);
        }
    }

    private double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    public Map<String, Bet> getBets() {
        return Map.copyOf(bets);
    }

    public static class FokNotFilledException extends IllegalStateException {
        public FokNotFilledException(String message) {
            super(message == null ? "FOK order was not completely filled" : message);
        }
    }
}