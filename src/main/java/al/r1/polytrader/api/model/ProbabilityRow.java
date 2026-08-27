package al.r1.polytrader.api.model;

import java.util.List;

public record ProbabilityRow(
        int seconds,
        List<ProbabilityBucket> buckets
) {}