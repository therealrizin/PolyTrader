package al.r1.polytrader.services;

import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.services.binance.BinanceService;
import al.r1.polytrader.services.binance.model.BinanceKline;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveMarketDataService {

    private final TaskScheduler liveDataTaskScheduler;
    private final BinanceService binanceService;
    private final PolymarketService polymarketService;
    private final ProbabilityTable probabilityTable;
    private final TradingDecisionService tradingDecisionService;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    private final RollingPriceWindow rollingWindow = new RollingPriceWindow();

    public void start() {
        ScheduledFuture<?> future = liveDataTaskScheduler.scheduleAtFixedRate(
                this::tick,
                Instant.now().plusSeconds(1),
                Duration.ofSeconds(1)
        );
        scheduledTask.set(future);
        log.info("Live market data collection started");
    }

    public void stop() {
        ScheduledFuture<?> future = scheduledTask.getAndSet(null);
        if (future != null) {
            future.cancel(false);
            log.info("Live market data collection stopped");
        }
    }

    private void tick() {
        try {

            //TODO LOGIC HERE

        } catch (Exception e) {
            log.error("Error during live data tick", e);
        }
    }
}