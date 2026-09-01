package al.r1.polytrader.api.model;

import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.PricePoint;
import al.r1.polytrader.services.model.Prices;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

/**
 * Deliberately minimal: price, 60s TWAP, and the last 60 recorded seconds
 * of both — no per-exchange fields, no blended averages.
 */
public record PricesResponse(
        ChainlinkSymbol symbol,
        BigDecimal price,
        BigDecimal avg60sPrice,
        List<PricePoint> last60Seconds
) {
    public static PricesResponse gatherPrices(Prices prices) {
        List<PricePoint> history = prices.getRecentHistory(symbol).stream()
                .sorted(Comparator.comparing(PricePoint::date, Comparator.reverseOrder()))
                .toList();

        return new PricesResponse(
                symbol,
                prices.getPrice(symbol),
                prices.getAvg60sPrice(symbol),
                history
        );
    }
}