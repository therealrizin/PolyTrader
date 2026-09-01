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
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveMarketDataService {

    private final Prices prices;
    private final TaskScheduler liveDataTaskScheduler;
    private final PolymarketService polymarketService;
    private final TradingDecisionService tradingDecisionService;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    public void start() {
        scheduledTask.set(
                liveDataTaskScheduler.scheduleAtFixedRate(
                        this::tick,
                        Instant.now().plusSeconds(1),
                        Duration.ofSeconds(1)
                )
        );
        polymarketService.start();

        liveDataTaskScheduler.schedule(
                () -> {
                    tradingDecisionService.start();
                    log.info("TradingDecisionService started after 2-minute warmup.");
                },
                Instant.now().plusSeconds(120)
        );

        log.info("Live market data collection started (Chainlink prices via Polymarket RTDS); " +
                "TradingDecisionService will start in 2 minutes.");
    }

    public void stop() {
        ScheduledFuture<?> future = scheduledTask.getAndSet(null);
        if (future != null) {
            future.cancel(false);
            log.info("Live market data collection stopped");
        }
        tradingDecisionService.stop();
        polymarketService.stop();
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