package al.r1.polytrader.services.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PricePoint(
        LocalDateTime date,
        BigDecimal price,
        BigDecimal avg60sPrice
) {}