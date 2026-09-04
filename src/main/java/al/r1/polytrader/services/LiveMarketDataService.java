package al.r1.polytrader.services;

import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveMarketDataService {

    private final Prices prices;
    private final TaskScheduler liveDataTaskScheduler;
    private final PolymarketService polymarketService;
    private final TradingDecisionService tradingDecisionService;
    private final BackfillService backfillService;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> tradingStartTask = new AtomicReference<>();
    private final AtomicBoolean serviceStarted = new AtomicBoolean(false);
    private final AtomicBoolean tradingStarted = new AtomicBoolean(false);

    public void start() {
        if (!serviceStarted.compareAndSet(false, true)) {
            log.info("LiveMarketDataService already started");
            return;
        }

        try {
            scheduledTask.set(
                    liveDataTaskScheduler.scheduleAtFixedRate(
                            this::tick,
                            Instant.now().plusSeconds(1),
                            Duration.ofSeconds(1)
                    )
            );

            polymarketService.start();

            tradingStartTask.set(
                    liveDataTaskScheduler.scheduleAtFixedRate(
                            this::startTradingWhenBackfillReady,
                            Instant.now().plusSeconds(1),
                            Duration.ofSeconds(1)
                    )
            );

            log.info("LiveMarketDataService started. Trading remains disabled until BackfillService completes successfully.");
        } catch (Exception e) {
            serviceStarted.set(false);
            stopInternal();
            throw e;
        }
    }

    public void stop() {
        if (!serviceStarted.compareAndSet(true, false)) {
            return;
        }
        stopInternal();
    }

    private void stopInternal() {
        ScheduledFuture<?> liveTask = scheduledTask.getAndSet(null);
        if (liveTask != null) {
            liveTask.cancel(false);
            log.info("Live market data collection stopped");
        }

        ScheduledFuture<?> startTask = tradingStartTask.getAndSet(null);
        if (startTask != null) {
            startTask.cancel(false);
            log.info("Trading readiness polling stopped");
        }

        if (tradingStarted.compareAndSet(true, false)) {
            try {
                tradingDecisionService.stop();
            } catch (Exception e) {
                log.error("Failed stopping TradingDecisionService", e);
            }
        }

        try {
            polymarketService.stop();
        } catch (Exception e) {
            log.error("Failed stopping PolymarketService", e);
        }
    }

    private void startTradingWhenBackfillReady() {
        if (!serviceStarted.get() || tradingStarted.get()) return;

        if (backfillService.hasFailed()) {
            log.error("BackfillService FAILED. Trading will remain DISABLED.");
            ScheduledFuture<?> task = tradingStartTask.getAndSet(null);
            if (task != null) task.cancel(false);
            return;
        }

        if (!backfillService.isCompleted()) return;

        if (!tradingStarted.compareAndSet(false, true)) return;

        ScheduledFuture<?> task = tradingStartTask.getAndSet(null);
        if (task != null) task.cancel(false);

        log.info("BackfillService completed successfully. Starting TradingDecisionService.");
        try {
            tradingDecisionService.start();
            log.info("TradingDecisionService started successfully.");
        } catch (Exception e) {
            tradingStarted.set(false);
            log.error("TradingDecisionService failed to start. Trading remains disabled.", e);
            // Do not continuously retry a potentially broken trading engine.
            ScheduledFuture<?> retryTask = tradingStartTask.getAndSet(null);
            if (retryTask != null) retryTask.cancel(false);
        }
    }

    private void tick() {
        try {
            LocalDateTime now = LocalDateTime.now();
            for (ChainlinkSymbol symbol : ChainlinkSymbol.values()) {
                prices.recordSnapshot(symbol, now);
            }
        } catch (Exception e) {
            log.error("Error during live data tick", e);
        }
    }
}