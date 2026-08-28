package al.r1.polytrader.api.model;

import al.r1.polytrader.services.model.Prices;

import java.math.BigDecimal;
import java.util.List;

public record PricesResponse(
        BigDecimal avgPrice,
        BigDecimal avg60sPrice,
        BigDecimal binancePrice,
        BigDecimal binanceAvg60sPrice,
        BigDecimal coinbasePrice,
        BigDecimal krakenPrice,
        BigDecimal bybitPrice,
        BigDecimal okxPrice,
        List<PriceHistoryPoint> last10Seconds
) {
    public record PriceHistoryPoint(long timestampMillis, BigDecimal polymarketPrice, BigDecimal avgPrice) {}

    public static PricesResponse gatherPrices(Prices prices) {
        List<PriceHistoryPoint> history = prices.getPolymarketRecentHistory().stream()
                .map(p -> new PriceHistoryPoint(p.timestampMillis(), p.polymarketPrice(), p.avgPrice()))
                .toList();

        return new PricesResponse(
                prices.getAvgPrice(),
                prices.getAvg60sPrice(),
                prices.getBinancePrice(),
                prices.getBinanceAvg60sPrice(),
                prices.getCoinbasePrice(),
                prices.getKrakenPrice(),
                prices.getBybitPrice(),
                prices.getOkxPrice(),
                history
        );
    }
}
