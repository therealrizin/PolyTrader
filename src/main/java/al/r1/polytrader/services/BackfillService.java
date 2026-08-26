package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.binance.BinanceService;
import al.r1.polytrader.services.binance.model.BinanceKline;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
public class BackfillService {

    BinanceService binanceService;
    ProbabilityTable probabilityTable;

    public BackfillService(BinanceService binanceService, ProbabilityTable probabilityTable) {
        this.binanceService = binanceService;
        this.probabilityTable = probabilityTable;
    }

    public void gatherAndAnalyze() {
        downloadTwoWeeks(CurrencyPairs.BTCUSD.getValue());
    }

    private void downloadTwoWeeks(String symbol) {

        long end = System.currentTimeMillis();
        long start = end - Duration.ofDays(14).toMillis();

        long current = start;
        List<BinanceKline> candles = new ArrayList<>();

        while (current < end) {

            long chunkEnd = Math.min(
                    current + Duration.ofMinutes(16).toMillis(),
                    end
            );

            candles.addAll(binanceService.getKlines(symbol, current, chunkEnd));

            current = chunkEnd + 1;
        }

        updateProbabilityData(candles);
    }

    private void updateProbabilityData(List<BinanceKline> candles) {
        for(BinanceKline kline : candles) {
            double avg = (kline.high().doubleValue() + kline.low().doubleValue()) / 2;
                    probabilityTable.updateProbabilitiesTable();
        }
    }

}
