package al.r1.polytrader.services.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PriceSummary(
        LocalDateTime date,
        BigDecimal polymarket,
        BigDecimal avg60sPrice,
        BigDecimal avgPrice,
        BigDecimal binance,
        BigDecimal coinbase,
        BigDecimal kraken,
        BigDecimal bybit,
        BigDecimal okx
) {}