package al.r1.polytrader.services;

import al.r1.polytrader.services.model.PriceSummary;
import al.r1.polytrader.services.model.PriceTickAggregators;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.model.TickAggregator;
import al.r1.polytrader.services.polymarket.PolymarketService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class LiveMarketDataService {

    private final Prices globalPrices;
    private final TaskScheduler liveDataTaskScheduler;
    private final PriceTickAggregators tickAggregators;
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
        tradingDecisionService.start();
        log.info("Live market data collection started");
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
            TickAggregator.FlushResult binanceFlush = tickAggregators.getBinance().flush();
            TickAggregator.FlushResult coinbaseFlush = tickAggregators.getCoinbase().flush();
            TickAggregator.FlushResult krakenFlush = tickAggregators.getKraken().flush();
            TickAggregator.FlushResult bybitFlush = tickAggregators.getBybit().flush();
            TickAggregator.FlushResult okxFlush = tickAggregators.getOkx().flush();

            globalPrices.recordPriceSnapshot(new PriceSummary(
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault()),
                    globalPrices.getPolymarketPrice(),
                    globalPrices.getAvg60sPrice(),
                    globalPrices.getAvgPrice(),
                    binanceFlush.averagePrice(),
                    coinbaseFlush.averagePrice(),
                    krakenFlush.averagePrice(),
                    bybitFlush.averagePrice(),
                    okxFlush.averagePrice()
            ));
        } catch (Exception e) {
            log.error("Error during live data tick", e);
        }
    }
}