package al.r1.polytrader.api;

import al.r1.polytrader.api.model.ProbabilityBucket;
import al.r1.polytrader.api.model.ProbabilityRow;
import al.r1.polytrader.api.model.ProbabilityTableResponse;
import al.r1.polytrader.engine.ProbabilityTable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/v1/data")
public class DataControllerV1 {

    private static final int MAX_SECONDS = 300;

    private final ProbabilityTable probabilityTable;

    public DataControllerV1(ProbabilityTable probabilityTable) {
        this.probabilityTable = probabilityTable;
    }

    @GetMapping("/table")
    public ProbabilityTableResponse getTable(
            @RequestParam(defaultValue = "1") int seconds
    ) {
        if (seconds < 1 || seconds > MAX_SECONDS) {
            throw new IllegalArgumentException(
                    "seconds must be between 1 and " + MAX_SECONDS);
        }

        double[][] table = probabilityTable.getProbabilitiesTable();
        double totalWeight = probabilityTable.getNumberOfChecksWithWeight();

        List<ProbabilityBucket> buckets = new ArrayList<>();
        for (int b = 0; b < table[seconds].length; b++) {
            double weighted = table[seconds][b];
            double probability = totalWeight > 0 ? weighted / totalWeight : 0.0;
            buckets.add(new ProbabilityBucket(bucketLabel(b), weighted, probability));
        }

        List<ProbabilityRow> rows = List.of(new ProbabilityRow(seconds, buckets));

        return new ProbabilityTableResponse(
                probabilityTable.getNumberOfChecks(),
                totalWeight,
                rows
        );
    }

    private String bucketLabel(int b) {
        if (b == 0) return "-3% or more";
        if (b == 60) return "+3% or more";
        if (b == 30) return "0%";
        double pct = (b - 30) * 0.1;
        return String.format("%+.1f%%", pct);
    }
}