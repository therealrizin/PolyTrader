package al.r1.polytrader.services.betting.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public record ExecutionOrderResponse(
        boolean success,
        @JsonProperty("order_id") String orderId,
        String status,
        String error,
        @JsonProperty("making_amount") BigDecimal makingAmount,
        @JsonProperty("taking_amount") BigDecimal takingAmount
) {
}