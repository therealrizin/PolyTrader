package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.engine.model.UpDownEvEstimate;
import al.r1.polytrader.services.betting.MockBetService;
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
    private final TradingProperties tradingProperties;
    private final TaskScheduler liveDataTaskScheduler;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

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

            if (mockBetService.hasOpenBetFor(snapshot.slug())) {
                logSkip("BET_ALREADY_OPEN", snapshot.slug(), "one bet per window already placed");
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

            UpDownEvEstimate estimate = tradingEngine.estimateUpDown(
                    currentLivePrice,
                    currentTwapPrice,
                    snapshot.strikePriceUsd(),
                    (int) snapshot.secondsUntilClose(),
                    upMarketPrice,
                    downMarketPrice,
                    tradingProperties.takerFee()
            );

            log.info("EVAL slug={} secondsUntilClose={} secondsSinceOpen={} currentPrice={} referencePrice(strike)={} " +
                            "upMarketPrice={} downMarketPrice={} upChance={} downChance={} upEv={} downEv={} " +
                            "recommendedSide={} recommendedChance={} recommendedEv={} thresholds(minWinChance={}, minEv={})",
                    snapshot.slug(), snapshot.secondsUntilClose(), snapshot.secondsSinceOpen(), currentPrice, snapshot.strikePriceUsd(),
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

            mockBetService.placeMockBet(
                    snapshot.slug(),
                    estimate.recommendedSide(),
                    currentPrice,
                    snapshot.strikePriceUsd(),
                    marketPriceForSide,
                    estimate.recommendedEv(),
                    estimate.recommendedChance(),
                    snapshot.secondsUntilClose()
            );

            log.info("DECISION action=BET_PLACED slug={} side={} amount={} priceBetAt={} priceToAchieve={} " +
                            "marketPriceForSide={} winChance={} ev={}",
                    snapshot.slug(), estimate.recommendedSide(), tradingProperties.betAmount(),
                    currentPrice, snapshot.strikePriceUsd(), marketPriceForSide,
                    round(estimate.recommendedChance()), round(estimate.recommendedEv()));
        } catch (Exception e) {
            log.error("Error during trading decision evaluation", e);
        }
    }

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