package al.r1.polytrader.services.betting.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExecutionOrderRequest(
        @JsonProperty("client_order_id") String clientOrderId,
        @JsonProperty("market_slug") String marketSlug,
        @JsonProperty("token_id") String tokenId,
        String side,
        String price,
        String size,
        @JsonProperty("amount_usdc") String amountUsdc,
        @JsonProperty("order_type") String orderType,
        @JsonProperty("amount") String amount
) {
}