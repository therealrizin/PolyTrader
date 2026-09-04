package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.binance.BinanceService;
import al.r1.polytrader.services.binance.model.BinanceKline;
import al.r1.polytrader.services.model.CurrencyPairs;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class BackfillService {
    private static final Logger log = LoggerFactory.getLogger(BackfillService.class);

    private final BinanceService binanceService;
    private final ProbabilityTable probabilityTable;
    private final TaskScheduler taskScheduler;

    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);

    public BackfillService(TaskScheduler taskScheduler, ProbabilityTable probabilityTable, BinanceService binanceService) {
        this.taskScheduler = taskScheduler;
        this.probabilityTable = probabilityTable;
        this.binanceService = binanceService;
    }

    @PostConstruct
    public void init() {
        taskScheduler.schedule(this::gatherAndAnalyze, Instant.now());
        log.info("BackfillService scheduled");
    }

    public void gatherAndAnalyze() {
        log.info("Starting historical backfill...");
        try {
            completed.set(false);
            failed.set(false);
            downloadTwoWeeks(CurrencyPairs.BTCUSDT.getValue());
            completed.set(true);
            log.info("Historical backfill completed successfully. Trading is now allowed to start.");
        } catch (Exception e) {
            failed.set(true);
            completed.set(false);
            log.error("Historical backfill FAILED. Trading will NOT be started.", e);
        }
    }

    public boolean isCompleted() {
        return completed.get();
    }

    public boolean hasFailed() {
        return failed.get();
    }

    private void downloadTwoWeeks(String symbol) {
        long end = System.currentTimeMillis();
        long start = end - Duration.ofDays(4).toMillis(); // TODO: change to 14 days when ready
        long current = start;
        List<BinanceKline> candles = new ArrayList<>();

        log.info("Starting Binance download. symbol={}, start={}, end={}", symbol, start, end);

        while (current < end) {
            long chunkEnd = Math.min(current + Duration.ofMinutes(16).toMillis(), end);
            log.info("Downloading chunk: {} -> {}", current, chunkEnd);
            List<BinanceKline> chunk = binanceService.getKlines(symbol, current, chunkEnd);
            if (chunk != null && !chunk.isEmpty()) {
                candles.addAll(chunk);
                log.info("Downloaded {} candles. Total: {}", chunk.size(), candles.size());
            } else {
                log.warn("Binance returned no candles for range {} -> {}", current, chunkEnd);
            }
            current = chunkEnd + 1;
        }

        log.info("Finished Binance download. Raw candles collected: {}", candles.size());
        updateProbabilityData(candles);
    }

    private void updateProbabilityData(List<BinanceKline> rawCandles) {
        List<BinanceKline> candles = validateAndCleanCandles(rawCandles);
        int n = candles.size();

        if (n == 0) {
            throw new IllegalStateException("No valid candles remaining after validation");
        }

        double[] closes = new double[n];
        long[] closeTimes = new long[n];
        for (int i = 0; i < n; i++) {
            BinanceKline k = candles.get(i);
            closes[i] = k.close().doubleValue();
            closeTimes[i] = k.closeTime();
        }

        double[] avg60 = new double[n];
        double windowSum = 0;
        int windowStart = 0;
        for (int i = 0; i < n; i++) {
            windowSum += closes[i];
            while (windowStart <= i && closeTimes[i] - closeTimes[windowStart] >= 60_000) {
                windowSum -= closes[windowStart];
                windowStart++;
            }
            int windowSize = i - windowStart + 1;
            avg60[i] = windowSum / windowSize;
        }

        log.info("Building probability table from {} validated candles...", n);

        for (int i = 0; i < n; i++) {
            double currentAvg = avg60[i];
            long currentTime = closeTimes[i];
            int lowerBound = Math.max(0, i - 300);
            probabilityTable.updateNumberOfChecks();

            for (int y = i - 1; y >= lowerBound; y--) {
                long elapsedSeconds = (currentTime - closeTimes[y]) / 1000;
                if (elapsedSeconds < 1 || elapsedSeconds > 300) continue;
                double changePure = currentAvg / avg60[y];
                probabilityTable.updateProbabilitiesTable((int) elapsedSeconds, changePure, false);
            }
        }

        log.info("Probability table successfully built from {} candles", n);
    }

    private List<BinanceKline> validateAndCleanCandles(List<BinanceKline> rawCandles) {
        if (rawCandles == null || rawCandles.isEmpty()) {
            log.warn("validateAndCleanCandles received null or empty input");
            return List.of();
        }

        int nullCount = 0, invalidPriceCount = 0, badTimeCount = 0;
        List<BinanceKline> filtered = new ArrayList<>(rawCandles.size());

        for (BinanceKline k : rawCandles) {
            if (k == null || k.close() == null || k.open() == null || k.high() == null || k.low() == null) {
                nullCount++;
                continue;
            }
            if (k.close().signum() <= 0 || k.open().signum() <= 0 || k.high().signum() <= 0 || k.low().signum() <= 0) {
                invalidPriceCount++;
                continue;
            }
            if (k.high().compareTo(k.low()) < 0 || k.close().compareTo(k.low()) < 0 || k.close().compareTo(k.high()) > 0) {
                invalidPriceCount++;
                continue;
            }
            if (k.closeTime() <= 0 || k.openTime() <= 0 || k.closeTime() < k.openTime()) {
                badTimeCount++;
                continue;
            }
            filtered.add(k);
        }

        filtered.sort(Comparator.comparingLong(BinanceKline::closeTime));

        List<BinanceKline> deduped = new ArrayList<>(filtered.size());
        long lastTime = -1;
        int duplicateCount = 0, gapCount = 0;

        for (BinanceKline k : filtered) {
            if (k.closeTime() == lastTime) {
                duplicateCount++;
                continue;
            }
            if (lastTime != -1) {
                long deltaSeconds = (k.closeTime() - lastTime) / 1000;
                if (deltaSeconds > 1) gapCount++;
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