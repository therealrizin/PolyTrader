package al.r1.polytrader.api.model;

import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.PricePoint;
import al.r1.polytrader.services.model.Prices;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

public record PricesResponse(
        ChainlinkSymbol symbol,
        BigDecimal price,
        BigDecimal avg60sPrice,
        List<PricePoint> last60Seconds
) {
    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;

    public static PricesResponse gatherPrices(Prices prices) {
        List<PricePoint> history = prices.getRecentHistory(SYMBOL).stream()
                .map(pp -> new PricePoint(
                        pp.timestamp(),
                        pp.price(),
                        prices.getAvg60sPrice(SYMBOL)
                ))
                .sorted(Comparator.comparing(PricePoint::date, Comparator.reverseOrder()))
                .toList();

        return new PricesResponse(
                SYMBOL,
                prices.getPrice(SYMBOL),
                prices.getAvg60sPrice(SYMBOL),
                history
        );
    }
}