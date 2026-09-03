package al.r1.polytrader.api.model;

import java.math.BigDecimal;
import java.util.List;

public record StrategiesResponse(
        List<StrategySummary> strategies,
        int totalBets,
        int openBets,
        int wonBets,
        int lostBets,
        int soldBets,
        BigDecimal netProfitLoss
) {}