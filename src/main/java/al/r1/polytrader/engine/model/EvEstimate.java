package al.r1.polytrader.engine.model;

public record EvEstimate(
        double stayEv,
        double changeEv,
        double estimatedChance,
        Side recommendedSide
) {}