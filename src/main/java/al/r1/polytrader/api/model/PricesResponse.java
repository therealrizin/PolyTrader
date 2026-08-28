package al.r1.polytrader.api.model;

import al.r1.polytrader.services.model.Prices;

import java.math.BigDecimal;

public record PricesResponse(
        BigDecimal avgPrice,
        BigDecimal avg60sPrice,
        BigDecimal polymarket60sAvgPrice,
        BigDecimal binancePrice,
        BigDecimal binanceAvg60sPrice,
        BigDecimal coinbasePrice,
        BigDecimal krakenPrice,
        BigDecimal bybitPrice,
        BigDecimal okxPrice
) {
    public static PricesResponse gatherPrices(Prices prices) {
        return new PricesResponse(
                prices.getAvgPrice(),
                prices.getAvg60sPrice(),
                prices.getPolymarket60sAvgPrice(),
                prices.getBinancePrice(),
                prices.getBinanceAvg60sPrice(),
                prices.getCoinbasePrice(),
                prices.getKrakenPrice(),
                prices.getBybitPrice(),
                prices.getOkxPrice()
        );
    }
}
