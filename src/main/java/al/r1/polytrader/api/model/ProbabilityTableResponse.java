package al.r1.polytrader.api.model;

import java.util.List;

public record ProbabilityTableResponse(
        int numberOfChecks,
        double numberOfChecksWithWeight,
        List<ProbabilityRow> rows
) {}