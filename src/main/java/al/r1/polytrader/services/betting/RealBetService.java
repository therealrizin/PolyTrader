package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.polymarket.PolymarketMarketResolver;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class RealBetService {

    private final TradingProperties tradingProperties;
    private final PolymarketMarketResolver marketResolver;
    private final WebClient executionWebClient;

    /** Prevents two strategy ticks from placing two bets on the same 5-minute market. */
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();

    /** Locally tracked real bets. Polymarket remains the source of truth for the actual order. */
    private final Map<String, RealBet> bets = new ConcurrentHashMap<>();

    public RealBetService(TradingProperties tradingProperties,
                          PolymarketMarketResolver marketResolver,
                          @Qualifier("executionWebClient") WebClient executionWebClient) {
        this.tradingProperties = tradingProperties;
        this.marketResolver = marketResolver;
        this.executionWebClient = executionWebClient;
    }

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public RealBet placeRealBet(PolymarketMarketSnapshot snapshot,
                                MarketSide side,
                                double countedEv,
                                double countedWinChance) {

        if (snapshot == null) {
            throw new IllegalArgumentException("Polymarket snapshot cannot be null");
        }

        String slug = snapshot.slug();
        if (slug == null || slug.isBlank()) {
            throw new IllegalArgumentException("Polymarket market slug cannot be empty");
        }

        // Never place more than one bet in the same market.
        if (!openSlugs.add(slug)) {
            throw new IllegalStateException("Already have an open real bet for market " + slug);
        }

        try {
            BigDecimal price = getExecutablePrice(snapshot, side);
            validatePrice(price);

            BigDecimal amount = tradingProperties.betAmount();
            if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalStateException("trading.bet-amount must be > 0");
            }

            PolymarketMarketResolver.ResolvedMarket market = marketResolver.resolveCurrentMarket()
                    .filter(m -> m.slug().equals(slug))
                    .orElseThrow(() -> new IllegalStateException(
                            "Could not resolve CLOB token IDs for " + slug
                                    + " (market resolver has no active/matching subscription)"));

            String tokenId = side == MarketSide.UP ? market.upTokenId() : market.downTokenId();

            /*
             * FOK limit order: price = current best ask, size = USDC amount / price.
             * - never pay more than the price observed by Java
             * - FOK prevents leaving a resting order behind
             * - if liquidity is unavailable, the order fails outright
             */
            BigDecimal size = amount.divide(price, 8, RoundingMode.DOWN);
            if (size.compareTo(BigDecimal.ZERO) <= 0) {
                throw new IllegalStateException("Calculated order size is zero");
            }

            String clientBetId = UUID.randomUUID().toString();

            /*
             * FIX: The order side must be "BUY" (or "SELL") for the specific token,
             * not the market side. Here we are buying the chosen token.
             */
            String orderSide = "BUY";

            ExecutionOrderRequest request = new ExecutionOrderRequest(
                    clientBetId, slug, tokenId, orderSide, price, size, amount, "FOK");

            log.info("REAL BET submitting: id={} slug={} side={} tokenId={} price={} size={} amount={} " +
                            "EV={} winChance={} secondsUntilClose={}",
                    clientBetId, slug, side, tokenId, price, size, amount,
                    countedEv, countedWinChance, snapshot.secondsUntilClose());

            ExecutionOrderResponse response = executionWebClient.post()
                    .uri("/order")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(ExecutionOrderResponse.class)
                    .block(Duration.ofSeconds(10));

            if (response == null) {
                throw new IllegalStateException("Execution service returned empty response");
            }
            if (!response.success()) {
                throw new IllegalStateException("Polymarket order rejected: " + response.error());
            }

            Instant placedAt = Instant.now();

            RealBet bet = new RealBet(
                    clientBetId, response.orderId(), slug, side, amount, price, size,
                    countedEv, countedWinChance, placedAt, snapshot.secondsUntilClose());

            bets.put(clientBetId, bet);

            log.info("REAL BET ACCEPTED: id={} orderId={} slug={} side={} price={} size={} amount={}",
                    clientBetId, response.orderId(), slug, side, price, size, amount);

            return bet;

        } catch (Exception e) {
            // Order wasn't accepted — free the slug up for another attempt in this market.
            openSlugs.remove(slug);
            log.error("REAL BET FAILED: slug={} side={}", slug, side, e);
            throw e;
        }
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

    public Map<String, RealBet> getBets() {
        return Map.copyOf(bets);
    }

    public record ExecutionOrderRequest(
            String clientOrderId,
            String marketSlug,
            String tokenId,
            String side,
            BigDecimal price,
            BigDecimal size,
            BigDecimal amountUsdc,
            String orderType
    ) {}

    public record ExecutionOrderResponse(
            boolean success,
            String orderId,
            String status,
            String error,
            BigDecimal makingAmount,
            BigDecimal takingAmount
    ) {}

    public record RealBet(
            String id,
            String orderId,
            String marketSlug,
            MarketSide side,
            BigDecimal amount,
            BigDecimal price,
            BigDecimal size,
            double countedEv,
            double countedWinChance,
            Instant placedAt,
            long secondsUntilClose
    ) {}
}