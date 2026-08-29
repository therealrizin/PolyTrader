package al.r1.polytrader.services;

import al.r1.polytrader.services.binance.BinanceService;
import al.r1.polytrader.services.model.CurrencyPairs;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveMarketDataService {

    private final Prices globalPrices;
    private final TaskScheduler liveDataTaskScheduler;
    private final BinanceService binanceService;
    private final PolymarketService polymarketService;
    private final TradingDecisionService tradingDecisionService;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    public void start() {
        ScheduledFuture<?> future = liveDataTaskScheduler.scheduleAtFixedRate(
                this::tick,
                Instant.now().plusSeconds(1),
                Duration.ofSeconds(1)
        );
        scheduledTask.set(future);
        polymarketService.start();
        log.info("Live market data collection started");
    }

    public void stop() {
        ScheduledFuture<?> future = scheduledTask.getAndSet(null);
        if (future != null) {
            future.cancel(false);
            log.info("Live market data collection stopped");
        }
        polymarketService.stop();
    }

    /**
     * Runs every 1s to keep Prices (and the API's last-10s snapshot list)
     * current. This does NOT drive ProbabilityTable anymore — live table
     * updates come from PolymarketTwapClient, keyed on Chainlink's own
     * observation timestamps, not this tick's cadence. Note: Polymarket's
     * RTDS push is not guaranteed to arrive every second (the 60s window
     * is a lookback size, not a publish cadence, per Polymarket's docs) —
     * this tick just samples whatever the latest known values are.
     */
    private void tick() {
        try {
            BigDecimal latest = binanceService.getLatestPrice().get(CurrencyPairs.BTCUSD);
            long timestampMillis = System.currentTimeMillis();
            if (latest != null) {
                globalPrices.setBinancePrice(latest);
            }
            globalPrices.recordPriceSnapshot(timestampMillis);
        } catch (Exception e) {
            log.error("Error during live data tick", e);
        }
    }
}