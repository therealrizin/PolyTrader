package al.r1.polytrader.api.model;

public record ProbabilityBucket(
        String change,
        double weightedCount,
        String probability
) {}
