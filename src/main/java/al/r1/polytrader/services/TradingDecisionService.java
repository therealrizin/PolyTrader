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
    private static final Duration SKIP_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    private final Prices prices;
    private final TradingEngine tradingEngine;
    private final PolymarketDataProvider marketDataProvider;
    private final MockBetService mockBetService;
    private final TradingProperties tradingProperties;
    private final TaskScheduler liveDataTaskScheduler;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    // De-duplication state for repetitive skip-reason logging.
    private final AtomicReference<String> lastSkipKey = new AtomicReference<>();
    private final AtomicReference<Instant> lastSkipHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

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
            if (snapshotOpt.isEmpty()) {
                logSkip("NO_SNAPSHOT", null, "no open Polymarket market snapshot yet");
                return;
            }

            PolymarketMarketSnapshot snapshot = snapshotOpt.get();

            if (snapshot.secondsUntilClose() < MIN_SECONDS_TO_ACT) {
                logSkip("TOO_CLOSE_TO_CLOSE", snapshot.slug(),
                        "secondsUntilClose=" + snapshot.secondsUntilClose() + " minSecondsToAct=" + MIN_SECONDS_TO_ACT);
                return;
            }

            if (mockBetService.hasOpenBetFor(snapshot.slug())) {
                logSkip("BET_ALREADY_OPEN", snapshot.slug(), "one bet per window already placed");
                return;
            }

            BigDecimal currentPrice = prices.getAvg60sPrice();
            if (currentPrice == null || currentPrice.signum() == 0) {
                logSkip("NO_CURRENT_PRICE", snapshot.slug(), "avg60sPrice not available yet");
                return;
            }

            if (snapshot.upPrice() == null || snapshot.downPrice() == null) {
                logSkip("MISSING_MARKET_PRICES", snapshot.slug(),
                        "upPrice=" + snapshot.upPrice() + " downPrice=" + snapshot.downPrice());
                return;
            }
            double upMarketPrice = snapshot.upPrice().doubleValue();
            double downMarketPrice = snapshot.downPrice().doubleValue();

            // Clear skip de‑dup state now that we have a full evaluation
            lastSkipKey.set(null);

            UpDownEvEstimate estimate = tradingEngine.estimateUpDown(
                    currentPrice,
                    snapshot.strikePriceUsd(),
                    (int) snapshot.secondsUntilClose(),
                    upMarketPrice,
                    downMarketPrice,
                    tradingProperties.takerFee()
            );

            // Full odds/prices trace
            log.info("EVAL slug={} secondsUntilClose={} currentPrice={} referencePrice(strike)={} " +
                            "upMarketPrice={} downMarketPrice={} upChance={} downChance={} upEv={} downEv={} " +
                            "recommendedSide={} recommendedChance={} recommendedEv={} thresholds(minWinChance={}, minEv={})",
                    snapshot.slug(), snapshot.secondsUntilClose(), currentPrice, snapshot.strikePriceUsd(),
                    upMarketPrice, downMarketPrice,
                    round(estimate.upChance()), round(estimate.downChance()),
                    round(estimate.upEv()), round(estimate.downEv()),
                    estimate.recommendedSide(), round(estimate.recommendedChance()), round(estimate.recommendedEv()),
                    tradingProperties.minimumWinChance(), tradingProperties.minimumExpectedEv());

            if (estimate.recommendedChance() < tradingProperties.minimumWinChance()) {
                log.info("DECISION skip reason=WIN_CHANCE_BELOW_THRESHOLD slug={} side={} winChance={} threshold={}",
                        snapshot.slug(), estimate.recommendedSide(),
                        round(estimate.recommendedChance()), tradingProperties.minimumWinChance());
                return;
            }
            if (estimate.recommendedEv() < tradingProperties.minimumExpectedEv()) {
                log.info("DECISION skip reason=EV_BELOW_THRESHOLD slug={} side={} ev={} threshold={}",
                        snapshot.slug(), estimate.recommendedSide(),
                        round(estimate.recommendedEv()), tradingProperties.minimumExpectedEv());
                return;
            }

            double marketPriceForSide = estimate.recommendedSide() == MarketSide.UP ? upMarketPrice : downMarketPrice;

            // Place the mock bet with the fixed amount from configuration
            mockBetService.placeMockBet(
                    snapshot.slug(),
                    estimate.recommendedSide(),
                    BigDecimal.valueOf(marketPriceForSide),   // priceBetAt = entry odds
                    snapshot.strikePriceUsd(),                // priceToAchieve = strike
                    marketPriceForSide,                       // marketPriceAtBet (for logging)
                    estimate.recommendedEv(),
                    estimate.recommendedChance(),
                    snapshot.secondsUntilClose()
            );

            log.info("DECISION action=BET_PLACED slug={} side={} amount={} priceBetAt={} priceToAchieve={} " +
                            "marketPriceForSide={} winChance={} ev={}",
                    snapshot.slug(), estimate.recommendedSide(), tradingProperties.mockBetAmount(),
                    marketPriceForSide, snapshot.strikePriceUsd(), marketPriceForSide,
                    round(estimate.recommendedChance()), round(estimate.recommendedEv()));
        } catch (Exception e) {
            log.error("Error during trading decision evaluation", e);
        }
    }

    /**
     * Logs a skip reason at INFO the first time this (reason, slug)
     * combination is seen, then drops to DEBUG for as long as the exact
     * same state repeats — with a periodic INFO heartbeat so the log
     * doesn't go completely silent for minutes at a time.
     */
    private void logSkip(String reason, String slug, String detail) {
        String key = reason + "|" + slug;
        String previousKey = lastSkipKey.getAndSet(key);
        boolean changed = !key.equals(previousKey);

        boolean heartbeatDue = Duration.between(lastSkipHeartbeatAt.get(), Instant.now())
                .compareTo(SKIP_HEARTBEAT_INTERVAL) >= 0;

        if (changed || heartbeatDue) {
            if (heartbeatDue) lastSkipHeartbeatAt.set(Instant.now());
            log.info("DECISION skip reason={} slug={} detail='{}'{}",
                    reason, slug, detail, changed ? "" : " (heartbeat, state unchanged)");
        } else {
            log.debug("DECISION skip reason={} slug={} detail='{}'", reason, slug, detail);
        }
    }

    private double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}