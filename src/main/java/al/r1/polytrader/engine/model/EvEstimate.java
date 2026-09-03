package al.r1.polytrader.engine.model;

public record EvEstimate(
        double upChance,
        double downChance,
        double upEv,
        double downEv,
        MarketSide recommendedSide,
        double recommendedChance,
        double recommendedEv
) {}