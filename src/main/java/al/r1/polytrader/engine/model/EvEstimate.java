package al.r1.polytrader.engine.model;

public record EvEstimate(
        double upChance,
        double upPriceToMeetEv,
        double upEvRequired,
        double downChance,
        double downPriceToMeetEv,
        double downEvRequired
) {}