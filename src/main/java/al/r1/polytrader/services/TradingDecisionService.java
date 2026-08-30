package al.r1.polytrader.services;

import al.r1.polytrader.config.trading.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
import al.r1.polytrader.services.betting.MockBetService;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketDataProvider;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ties the live price feeds, the Polymarket 5-min up/down snapshot, and
 * TradingEngine together into a once-per-second check-and-decide loop.
 * Only ever places {@link MockBetService} bets — no real trading is wired
 * up here, regardless of trading.mock.
 */
@Slf4j
@Service
public class TradingDecisionService {

    private static final int MIN_SECONDS_TO_ACT = 2;

    private final Prices prices;
    private final TradingEngine tradingEngine;
    private final PolymarketDataProvider marketDataProvider;
    private final MockBetService mockBetService;
    private final TradingProperties tradingProperties;
    private final TaskScheduler liveDataTaskScheduler;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    public TradingDecisionService(Prices prices,
                                  TradingEngine tradingEngine,
                                  PolymarketDataProvider marketDataProvider,
                                  MockBetService mockBetService,
                                  TradingProperties tradingProperties,
                                  TaskScheduler liveDataTaskScheduler) {
        this.prices = prices;
        this.tradingEngine = tradingEngine;
        this.marketDataProvider = marketDataProvider;
        this.mockBetService = mockBetService;
        this.tradingProperties = tradingProperties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
    }

    public void start() {
        ScheduledFuture<?> future = liveDataTaskScheduler.scheduleAtFixedRate(
                this::evaluateAndMaybeBet,
                Instant.now().plusSeconds(1),
                Duration.ofSeconds(1)
        );
        scheduledTask.set(future);
        log.info("Trading decision loop started (mock={}, minEv={}, minWinChance={}, takerFee={})",
                tradingProperties.mock(), tradingProperties.minimumExpectedEv(),
                tradingProperties.minimumWinChance(), tradingProperties.takerFee());
    }

    public void stop() {
        ScheduledFuture<?> future = scheduledTask.getAndSet(null);
        if (future != null) {
            future.cancel(false);
            log.info("Trading decision loop stopped");
        }
    }

    private void evaluateAndMaybeBet() {
        try {
            mockBetService.settleDueBets(prices);

            Optional<PolymarketMarketSnapshot> snapshotOpt = marketDataProvider.currentSnapshot();
            if (snapshotOpt.isEmpty()) return;

            PolymarketMarketSnapshot snapshot = snapshotOpt.get();

            if (snapshot.secondsUntilClose() < MIN_SECONDS_TO_ACT) return;
            if (mockBetService.hasOpenBetFor(snapshot.slug())) return; // one bet per window

            BigDecimal currentPrice = prices.getAvg60sPrice();
            if (currentPrice == null || currentPrice.signum() == 0) return;

            if (snapshot.upPrice() == null || snapshot.downPrice() == null) return;
            double upMarketPrice = snapshot.upPrice().doubleValue();
            double downMarketPrice = snapshot.downPrice().doubleValue();

            UpDownEvEstimate estimate = tradingEngine.estimateUpDown(
                    currentPrice,
                    snapshot.strikePriceUsd(),
                    (int) snapshot.secondsUntilClose(),
                    upMarketPrice,
                    downMarketPrice,
                    tradingProperties.takerFee()
            );

            if (estimate.recommendedChance() < tradingProperties.minimumWinChance()) {
                log.debug("Skipping {}: win chance {} below threshold {}",
                        snapshot.slug(), estimate.recommendedChance(), tradingProperties.minimumWinChance());
                return;
            }
            if (estimate.recommendedEv() < tradingProperties.minimumExpectedEv()) {
                log.debug("Skipping {}: EV {} below threshold {}",
                        snapshot.slug(), estimate.recommendedEv(), tradingProperties.minimumExpectedEv());
                return;
            }

            double marketPriceForSide = estimate.recommendedSide() == MarketSide.UP ? upMarketPrice : downMarketPrice;

            mockBetService.placeMockBet(
                    snapshot.slug(),
                    estimate.recommendedSide(),
                    currentPrice,
                    snapshot.strikePriceUsd(),
                    marketPriceForSide,
                    estimate.recommendedEv(),
                    estimate.recommendedChance(),
                    snapshot.secondsUntilClose(),
                    tradingProperties.mockBetAmount(),
                    tradingProperties.takerFee()
            );
        } catch (Exception e) {
            log.error("Error during trading decision evaluation", e);
        }
    }
}