package al.r1.polytrader.api;

import al.r1.polytrader.api.model.PricesResponse;
import al.r1.polytrader.api.model.ProbabilityBucket;
import al.r1.polytrader.api.model.ProbabilityRow;
import al.r1.polytrader.api.model.ProbabilityTableResponse;
import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.model.Prices;
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
    private static final int CENTER = 500; // index of 0.000% bucket

    private final ProbabilityTable probabilityTable;
    private final Prices prices;

    public DataControllerV1(ProbabilityTable probabilityTable, Prices prices) {
        this.probabilityTable = probabilityTable;
        this.prices = prices;
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
            double probabilityPct = totalWeight > 0 ? (weighted / totalWeight) * 100.0 : 0.0;

            buckets.add(new ProbabilityBucket(
                    bucketLabel(b),
                    round2(weighted),
                    round2(probabilityPct) + "%"
            ));
        }

        List<ProbabilityRow> rows = List.of(new ProbabilityRow(seconds, buckets));

        return new ProbabilityTableResponse(
                probabilityTable.getNumberOfChecks(),
                totalWeight,
                rows
        );
    }

    @GetMapping("/prices")
    public PricesResponse PricesResponse() {
        return PricesResponse.gatherPrices(prices);
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private String bucketLabel(int b) {
        if (b == 0) return "-0.500% or more";
        if (b == 1000) return "+0.500% or more";
        double pct = (b - CENTER) * 0.001;
        return String.format("%+.3f%%", pct);
    }
}