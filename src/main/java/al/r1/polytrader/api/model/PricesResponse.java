package al.r1.polytrader.api.model;

import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.model.PriceSummary;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

public record PricesResponse(
        BigDecimal avgPrice,
        BigDecimal avg60sPrice,
        BigDecimal polymarketPrice,
        BigDecimal binancePrice,
        BigDecimal binanceAvg60sPrice,
        BigDecimal coinbasePrice,
        BigDecimal krakenPrice,
        BigDecimal bybitPrice,
        BigDecimal okxPrice,
        List<PriceHistoryPoint> last60Seconds
) {
    public record PriceHistoryPoint(
            LocalDateTime date,
            BigDecimal polymarketPrice,
            BigDecimal avg60sPrice,
            BigDecimal avgPrice
    ) {}

    public static PricesResponse gatherPrices(Prices prices) {
        List<PriceHistoryPoint> history = prices.getRecentHistory().stream()
                .map(PricesResponse::toHistoryPoint)
                .sorted(Comparator.comparing(
                        PricesResponse.PriceHistoryPoint::date,
                        Comparator.reverseOrder()
                ))
                .toList();

        return new PricesResponse(
                prices.getAvgPrice(),
                prices.getAvg60sPrice(),
                prices.getPolymarketPrice(),
                prices.getBinancePrice(),
                prices.getBinanceAvg60sPrice(),
                prices.getCoinbasePrice(),
                prices.getKrakenPrice(),
                prices.getBybitPrice(),
                prices.getOkxPrice(),
                history
        );
    }

    private static PriceHistoryPoint toHistoryPoint(PriceSummary s) {
        return new PriceHistoryPoint(
                s.date(),
                s.polymarket(),
                s.avg60sPrice(),
                s.avgPrice()
        );
    }
}