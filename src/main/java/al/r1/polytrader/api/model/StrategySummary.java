package al.r1.polytrader.api.model;

import al.r1.polytrader.services.betting.model.MockBet;
import java.math.BigDecimal;
import java.util.List;

public record StrategySummary(
        String strategyId,
        double minEv,
        double minWinChance,
        int totalBets,
        int openBets,
        int wonBets,
        int lostBets,
        int soldBets,
        BigDecimal netProfitLoss
) {}