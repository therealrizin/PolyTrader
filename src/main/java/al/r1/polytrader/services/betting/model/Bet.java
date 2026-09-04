package al.r1.polytrader.services.betting.model;

import al.r1.polytrader.engine.model.MarketSide;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public record Bet(
        String id,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("token_id") String tokenId,
        @JsonProperty("market_slug") String marketSlug,
        MarketSide side,
        BigDecimal amount,
        BigDecimal price,
        BigDecimal size,
        @JsonProperty("counted_ev") double countedEv,
        @JsonProperty("counted_win_chance") double countedWinChance,
        @JsonProperty("placed_at") Instant placedAt,
        @JsonProperty("seconds_until_close") long secondsUntilClose,
        BetStatus status,
        @JsonProperty("sell_order_id") String sellOrderId,
        @JsonProperty("sold_price") BigDecimal soldPrice,
        @JsonProperty("profit_loss") BigDecimal profitLoss
) {
}