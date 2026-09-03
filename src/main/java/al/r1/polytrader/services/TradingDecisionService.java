package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.services.betting.MockBetService;
import al.r1.polytrader.services.betting.RealBetService;
import al.r1.polytrader.services.betting.model.MockBet;
import al.r1.polytrader.services.betting.model.RealBet;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketDataProvider;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class TradingDecisionService {

    private static final int MIN_SECONDS_TO_ACT = 10;
    private static final Duration SKIP_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;

    private final Prices prices;
    private final TradingEngine tradingEngine;
    private final PolymarketDataProvider marketDataProvider;
    private final MockBetService mockBetService;
    private final RealBetService realBetService;
    private final TradingProperties tradingProperties;
    private final TaskScheduler liveDataTaskScheduler;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

    // Buy-side skip logging state
    private final AtomicReference<String> lastSkipKey = new AtomicReference<>();
    private final AtomicReference<Instant> lastSkipHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    // Sell-side skip logging state
    private final AtomicReference<String> lastSellSkipKey = new AtomicReference<>();
    private final AtomicReference<Instant> lastSellSkipHeartbeatAt = new AtomicReference<>(Instant.EPOCH);

    public TradingDecisionService(Prices prices, TradingEngine tradingEngine, PolymarketDataProvider marketDataProvider, MockBetService mockBetService, RealBetService realBetService, TradingProperties tradingProperties, TaskScheduler liveDataTaskScheduler) {
        this.prices = prices;
        this.tradingEngine = tradingEngine;
        this.marketDataProvider = marketDataProvider;
        this.mockBetService = mockBetService;
        this.realBetService = realBetService;
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
        log.info("Trading decision loop started (mock={}, minEv={}, minWinChance={}, takerFee={}, minSecondsSinceOpen={})",
                tradingProperties.mock(), tradingProperties.minimumExpectedEv(),
                tradingProperties.minimumWinChance(), tradingProperties.takerFee(),
                tradingProperties.minimumSecondsSinceOpen());
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
            mockBetService.settleDueBets();

            Optional<PolymarketMarketSnapshot> snapshotOpt = marketDataProvider.currentSnapshot();
            if (snapshotOpt.isEmpty()) {
                logSkip("NO_SNAPSHOT", null, "no open Polymarket market snapshot yet");
                return;
            }

            PolymarketMarketSnapshot snapshot = snapshotOpt.get();

            // ------------------------------------------------------------
            // SELL LOGIC – multi‑strategy for mock, single for real
            // ------------------------------------------------------------
            if (tradingProperties.mock()) {
                List<MockBet> soldBets = mockBetService.evaluateSellForAllStrategies(snapshot);
                if (!soldBets.isEmpty()) {
                    for (MockBet bet : soldBets) {
                        log.info("DECISION action=SOLD (MOCK) strategy={} slug={} side={} boughtAt={} soldAt={} profitLoss={}",
                                bet.strategyId(), bet.marketSlug(), bet.side(),
                                bet.marketPriceAtBet(), bet.priceAtResolution(), bet.profitLoss());
                    }
                } else {
                    logSellSkip("EV_BELOW_THRESHOLD", snapshot.slug(), "no mock strategy met sell EV");
                }
            } else {
                // Real mode: single bet handling
                if (!realBetService.hasOpenBetFor(snapshot.slug())) {
                    logSellSkip("NO_OPEN_BET", snapshot.slug(), "real mode");
                } else {
                    Optional<RealBet> sold = realBetService.sellOpenPosition(snapshot);
                    sold.ifPresentOrElse(
                            bet -> log.info("DECISION action=SOLD (REAL) slug={} side={} boughtAt={} soldAt={} profitLoss={}",
                                    bet.marketSlug(), bet.side(), bet.price(),
                                    bet.soldPrice(), bet.profitLoss()),
                            () -> logSellSkip("EV_BELOW_THRESHOLD", snapshot.slug(), "real sell check returned empty")
                    );
                }
            }

            // ------------------------------------------------------------
            // COMMON ENTRY PRE‑CHECKS (time, price, market data)
            // ------------------------------------------------------------
            if (snapshot.secondsSinceOpen() < tradingProperties.minimumSecondsSinceOpen()) {
                logSkip("TOO_SOON_AFTER_OPEN", snapshot.slug(),
                        "secondsSinceOpen=" + snapshot.secondsSinceOpen()
                                + " minimumSecondsSinceOpen=" + tradingProperties.minimumSecondsSinceOpen());
                return;
            }

            if (snapshot.secondsUntilClose() < MIN_SECONDS_TO_ACT) {
                logSkip("TOO_CLOSE_TO_CLOSE", snapshot.slug(),
                        "secondsUntilClose=" + snapshot.secondsUntilClose() + " minSecondsToAct=" + MIN_SECONDS_TO_ACT);
                return;
            }

            if (snapshot.secondsSinceOpen() < MIN_SECONDS_TO_ACT) {
                logSkip("TOO_EARLY_TO_BET", snapshot.slug(),
                        "secondsSinceOpen=" + snapshot.secondsSinceOpen() + " minSecondsToAct=" + MIN_SECONDS_TO_ACT);
                return;
            }

            // For real mode, we block if there's already an open real bet.
            // For mock mode, we allow multiple strategies to bet on the same slug,
            // so we skip this check only for mock.
            if (!tradingProperties.mock() && realBetService.hasOpenBetFor(snapshot.slug())) {
                logSkip("BET_ALREADY_OPEN", snapshot.slug(), "one real bet per window already placed");
                return;
            }

            BigDecimal currentPrice = prices.getPrice(SYMBOL);
            if (currentPrice == null || currentPrice.signum() == 0) {
                logSkip("NO_CURRENT_PRICE", snapshot.slug(), "Chainlink 60s TWAP not available yet");
                return;
            }

            if (snapshot.upPrice() == null || snapshot.downPrice() == null) {
                logSkip("MISSING_MARKET_PRICES", snapshot.slug(),
                        "upPrice=" + snapshot.upPrice() + " downPrice=" + snapshot.downPrice());
                return;
            }
            double upMarketPrice = snapshot.upPrice().doubleValue();
            double downMarketPrice = snapshot.downPrice().doubleValue();

            lastSkipKey.set(null);

            BigDecimal currentLivePrice = prices.getPrice(SYMBOL);
            BigDecimal currentTwapPrice = prices.getAvg60sPrice(SYMBOL);

            EvEstimate estimate = tradingEngine.estimateUpDown(
                    currentLivePrice,
                    currentTwapPrice,
                    snapshot.strikePriceUsd(),
                    (int) snapshot.secondsUntilClose(),
                    upMarketPrice,
                    downMarketPrice,
                    tradingProperties.takerFee()
            );

            log.info("******************************\nEVALUATION slug={}\nsecondsUntilClose={}\nsecondsSinceOpen={}\n\ncurrentPrice={}\nreferencePrice(strike)={}\n" +
                            "upMarketPrice={}\ndownMarketPrice={}\nupChance={}\nupEv={}\ndownChance={}\ndownEv={}" +
                            "\n\nrecommendedSide={}\nbestChance={}\nbestEv={}\n******************************)",
                    snapshot.slug(), snapshot.secondsUntilClose(), snapshot.secondsSinceOpen(), currentPrice, snapshot.strikePriceUsd(),
                    upMarketPrice, downMarketPrice,
                    round(estimate.upChance()), round(estimate.upEv()),
                    round(estimate.downChance()), round(estimate.downEv()),
                    estimate.recommendedSide(), round(estimate.recommendedChance()), round(estimate.recommendedEv()));

            if (estimate.recommendedChance() < tradingProperties.minimumWinChance()) {
                log.info("DECISION skip reason=WIN_CHANCE_BELOW_THRESHOLD slug={} side={} winChance={} threshold={}",
                        snapshot.slug(), estimate.recommendedSide(),
                        round(estimate.recommendedChance()), tradingProperties.minimumWinChance());
                return;
            }

            // Dynamic EV threshold based on win chance
            double effectiveEvThreshold = getEffectiveEvThreshold(estimate.recommendedChance());
            if (estimate.recommendedEv() < effectiveEvThreshold) {
                log.info("DECISION skip reason=EV_BELOW_THRESHOLD slug={} side={} ev={} threshold={} (winChance={})",
                        snapshot.slug(), estimate.recommendedSide(),
                        round(estimate.recommendedEv()), round(effectiveEvThreshold),
                        round(estimate.recommendedChance()));
                return;
            }

            double marketPriceForSide = estimate.recommendedSide() == MarketSide.UP ? upMarketPrice : downMarketPrice;

            // ------------------------------------------------------------
            // ENTRY LOGIC – multi‑strategy for mock, single for real
            // ------------------------------------------------------------
            if (tradingProperties.mock()) {
                List<MockBet> placed = mockBetService.placeBetsForAllStrategies(
                        snapshot,
                        estimate.recommendedSide(),
                        estimate.recommendedEv(),
                        estimate.recommendedChance(),
                        BigDecimal.valueOf(marketPriceForSide),
                        currentPrice,
                        snapshot.strikePriceUsd(),
                        snapshot.secondsUntilClose()
                );

                if (!placed.isEmpty()) {
                    for (MockBet bet : placed) {
                        log.info("DECISION action=BET_PLACED (MOCK) strategy={} slug={} side={} amount={} winChance={} ev={} effectiveThreshold={}",
                                bet.strategyId(), snapshot.slug(), estimate.recommendedSide(),
                                tradingProperties.betAmount(),
                                round(estimate.recommendedChance()), round(estimate.recommendedEv()),
                                round(effectiveEvThreshold));
                    }
                } else {
                    log.info("DECISION skip reason=NO_STRATEGY_QUALIFIED slug={} side={} ev={} winChance={}",
                            snapshot.slug(), estimate.recommendedSide(),
                            round(estimate.recommendedEv()), round(estimate.recommendedChance()));
                }
            } else {
                // Real mode: single bet
                try {
                    realBetService.placeRealBet(
                            snapshot, estimate.recommendedSide(), estimate.recommendedEv(), estimate.recommendedChance());

                    log.info("DECISION action=BET_PLACED (REAL) slug={} side={} amount={} winChance={} ev={} effectiveThreshold={}",
                            snapshot.slug(), estimate.recommendedSide(), tradingProperties.betAmount(),
                            round(estimate.recommendedChance()), round(estimate.recommendedEv()),
                            round(effectiveEvThreshold));
                } catch (Exception e) {
                    log.error("DECISION action=BET_FAILED (REAL) slug={} side={}",
                            snapshot.slug(), estimate.recommendedSide(), e);
                }
            }

        } catch (Exception e) {
            log.error("Error during trading decision evaluation", e);
        }
    }

    private double getEffectiveEvThreshold(double winChance) {
        double minEv = tradingProperties.minimumExpectedEv();
        double minWinChance = tradingProperties.minimumWinChance();

        if (winChance <= minWinChance) {
            return minEv;
        }

        if (winChance >= 0.90) {
            return minEv / 5.0;
        }

        double progress = (winChance - minWinChance) / (0.90 - minWinChance);

        return minEv * (1.0 - progress * 0.8);
    }

    // Buy-side skip logging
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

    // Sell-side skip logging
    private void logSellSkip(String reason, String slug, String detail) {
        String key = reason + "|" + slug;
        String previousKey = lastSellSkipKey.getAndSet(key);
        boolean changed = !key.equals(previousKey);

        boolean heartbeatDue = Duration.between(lastSellSkipHeartbeatAt.get(), Instant.now())
                .compareTo(SKIP_HEARTBEAT_INTERVAL) >= 0;

        if (changed || heartbeatDue) {
            if (heartbeatDue) lastSellSkipHeartbeatAt.set(Instant.now());
            log.info("SELL DECISION skip reason={} slug={} detail='{}'{}",
                    reason, slug, detail, changed ? "" : " (heartbeat, state unchanged)");
        } else {
            log.debug("SELL DECISION skip reason={} slug={} detail='{}'", reason, slug, detail);
        }
    }

    private double round(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}