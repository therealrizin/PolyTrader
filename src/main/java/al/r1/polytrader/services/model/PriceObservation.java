package al.r1.polytrader.services.model;

import java.math.BigDecimal;

public record PriceObservation(
        long observedAtMillis,
        BigDecimal price
) {}