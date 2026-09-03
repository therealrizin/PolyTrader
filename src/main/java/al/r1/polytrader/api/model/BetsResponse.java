package al.r1.polytrader.api.model;

import al.r1.polytrader.services.betting.model.MockBet;

import java.math.BigDecimal;
import java.util.List;

public record BetsResponse(
        int totalBets,
        int openBets,
        int wonBets,
        int lostBets,
        int soldBets,
        BigDecimal netProfitLoss,
        List<MockBet> bets
) {}