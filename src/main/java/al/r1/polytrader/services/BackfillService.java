package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.binance.BinanceService;
import al.r1.polytrader.services.binance.model.BinanceKline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class BackfillService {

    BinanceService binanceService;
    ProbabilityTable probabilityTable;

    private static final Logger log = LoggerFactory.getLogger(BackfillService.class);

    public BackfillService(BinanceService binanceService, ProbabilityTable probabilityTable) {
        this.binanceService = binanceService;
        this.probabilityTable = probabilityTable;
    }

    public void gatherAndAnalyze() {
        downloadTwoWeeks(CurrencyPairs.BTCUSD.getValue());
    }

    private void downloadTwoWeeks(String symbol) {

        long end = System.currentTimeMillis();
        //TODO change to 14 days later
        long start = end - Duration.ofDays(1).toMillis();

        long current = start;
        List<BinanceKline> candles = new ArrayList<>();

        log.info("Starting to gather base data from Binance");

        while (current < end) {

            long chunkEnd = Math.min(
                    current + Duration.ofMinutes(16).toMillis(),
                    end
            );

            candles.addAll(binanceService.getKlines(symbol, current, chunkEnd));

            current = chunkEnd + 1;
            log.info("Gathering data, current: " + current);
        }

        updateProbabilityData(candles);
    }

    private void updateProbabilityData(List<BinanceKline> rawCandles) {
        List<BinanceKline> candles = validateAndCleanCandles(rawCandles);
        int n = candles.size();

        if (n == 0) {
            log.warn("No valid candles remaining after validation, skipping probability update");
            return;
        }

        double[] closes = new double[n];
        long[] closeTimes = new long[n];
        for (int i = 0; i < n; i++) {
            BinanceKline k = candles.get(i);
            closes[i] = k.close().doubleValue();
            closeTimes[i] = k.closeTime();
        }

        // Rolling 60s average price, computed once via a sliding window
        double[] avg60 = new double[n];
        double windowSum = 0;
        int windowStart = 0;
        for (int i = 0; i < n; i++) {
            windowSum += closes[i];
            while (closeTimes[i] - closeTimes[windowStart] >= 60_000) {
                windowSum -= closes[windowStart];
                windowStart++;
            }
            int windowSize = i - windowStart + 1;
            avg60[i] = windowSum / windowSize;
        }

        for (int i = 0; i < n; i++) {
            double currentAvg = avg60[i];
            long currentTime = closeTimes[i];
            int lowerBound = Math.max(0, i - 300);

            probabilityTable.updateNumberOfChecks();
            for (int y = i - 1; y >= lowerBound; y--) {
                long elapsedSeconds = (currentTime - closeTimes[y]) / 1000;

                if (elapsedSeconds < 1 || elapsedSeconds > 300) {
                    continue;
                }

                double changePure = currentAvg / avg60[y];
                probabilityTable.updateProbabilitiesTable((int) elapsedSeconds, changePure, false);
            }
        }
    }

    /**
     * Filters out malformed candles, sorts by closeTime, drops exact duplicates,
     * and logs any time gaps so bad upstream data doesn't silently distort the
     * rolling average or the probability table.
     */
    private List<BinanceKline> validateAndCleanCandles(List<BinanceKline> rawCandles) {
        if (rawCandles == null || rawCandles.isEmpty()) {
            log.warn("validateAndCleanCandles received null or empty input");
            return List.of();
        }

        int nullCount = 0;
        int invalidPriceCount = 0;
        int badTimeCount = 0;

        List<BinanceKline> filtered = new ArrayList<>(rawCandles.size());

        for (BinanceKline k : rawCandles) {
            if (k == null || k.close() == null || k.open() == null
                    || k.high() == null || k.low() == null) {
                nullCount++;
                continue;
            }

            if (k.close().signum() <= 0 || k.open().signum() <= 0
                    || k.high().signum() <= 0 || k.low().signum() <= 0) {
                invalidPriceCount++;
                continue;
            }

            // sanity: high should be >= low, and close should sit within [low, high]
            if (k.high().compareTo(k.low()) < 0
                    || k.close().compareTo(k.low()) < 0
                    || k.close().compareTo(k.high()) > 0) {
                invalidPriceCount++;
                continue;
            }

            if (k.closeTime() <= 0 || k.openTime() <= 0 || k.closeTime() < k.openTime()) {
                badTimeCount++;
                continue;
            }

            filtered.add(k);
        }

        // Sort defensively — chunked downloads can arrive out of order
        filtered.sort(Comparator.comparingLong(BinanceKline::closeTime));

        // Drop exact duplicate timestamps (keep first occurrence)
        List<BinanceKline> deduped = new ArrayList<>(filtered.size());
        long lastTime = -1;
        int duplicateCount = 0;
        int gapCount = 0;

        for (BinanceKline k : filtered) {
            if (k.closeTime() == lastTime) {
                duplicateCount++;
                continue;
            }
            if (lastTime != -1) {
                long deltaSeconds = (k.closeTime() - lastTime) / 1000;
                if (deltaSeconds > 1) {
                    gapCount++;
                }
            }
            deduped.add(k);
            lastTime = k.closeTime();
        }

        if (nullCount > 0) log.warn("Dropped {} candles with null fields", nullCount);
        if (invalidPriceCount > 0) log.warn("Dropped {} candles with invalid/inconsistent prices", invalidPriceCount);
        if (badTimeCount > 0) log.warn("Dropped {} candles with invalid timestamps", badTimeCount);
        if (duplicateCount > 0) log.warn("Dropped {} duplicate-timestamp candles", duplicateCount);
        if (gapCount > 0) log.warn("Detected {} time gaps (>1s) in candle series", gapCount);

        log.info("Validated candles: {} in -> {} out", rawCandles.size(), deduped.size());

        return deduped;
    }

}
