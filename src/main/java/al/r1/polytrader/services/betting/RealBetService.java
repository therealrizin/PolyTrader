package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.model.MarketSide;
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

            ExecutionOrderRequest request =
                    new ExecutionOrderRequest(
                            clientBetId,
                            slug,
                            tokenId,
                            "BUY",
                            price.toPlainString(),
                            size.toPlainString(),
                            amountUsdc.toPlainString(),
                            "FOK"
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

            RealBet bet =
                    new RealBet(
                            clientBetId,
                            response.orderId(),
                            slug,
                            side,
                            amount,
                            price,
                            size,
                            countedEv,
                            countedWinChance,
                            placedAt,
                            snapshot.secondsUntilClose()
                    );

            bets.put(
                    clientBetId,
                    bet);

            log.info(
                    "REAL BET ACCEPTED: id={} orderId={} slug={} side={} price={} size={} amount={}",
                    clientBetId,
                    response.orderId(),
                    slug,
                    side,
                    price,
                    size,
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
            String orderType
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
            long secondsUntilClose
    ) {
    }
}