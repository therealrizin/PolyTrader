package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.MarketSide;
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

    private final AtomicReference<ScheduledFuture<?>> scheduledTask =
            new AtomicReference<>();

    private final AtomicReference<String> lastSkipKey =
            new AtomicReference<>();

    private final AtomicReference<Instant> lastSkipHeartbeatAt =
            new AtomicReference<>(Instant.EPOCH);

    private final AtomicReference<String> lastSellSkipKey =
            new AtomicReference<>();

    private final AtomicReference<Instant> lastSellSkipHeartbeatAt =
            new AtomicReference<>(Instant.EPOCH);

    public TradingDecisionService(
            Prices prices,
            TradingEngine tradingEngine,
            PolymarketDataProvider marketDataProvider,
            MockBetService mockBetService,
            RealBetService realBetService,
            TradingProperties tradingProperties,
            TaskScheduler liveDataTaskScheduler) {

        this.prices = prices;
        this.tradingEngine = tradingEngine;
        this.marketDataProvider = marketDataProvider;
        this.mockBetService = mockBetService;
        this.realBetService = realBetService;
        this.tradingProperties = tradingProperties;
        this.liveDataTaskScheduler = liveDataTaskScheduler;
    }

    public void start() {

        ScheduledFuture<?> future =
                liveDataTaskScheduler.scheduleAtFixedRate(
                        this::periodicTick,
                        Instant.now().plusSeconds(1),
                        Duration.ofSeconds(1)
                );

        scheduledTask.set(future);

        log.info(
                "Trading sell loop started (mock={}, minEv={}, minWinChance={}, takerFee={}, minSecondsSinceOpen={})",
                tradingProperties.mock(),
                tradingProperties.minimumExpectedEv(),
                tradingProperties.minimumWinChance(),
                tradingProperties.takerFee(),
                tradingProperties.minimumSecondsSinceOpen()
        );
    }

    public void stop() {

        ScheduledFuture<?> future =
                scheduledTask.getAndSet(null);

        if (future != null) {
            future.cancel(false);

            log.info(
                    "Trading decision loop stopped"
            );
        }
    }

    /**
     * Called immediately after a fresh Chainlink BTC price update.
     *
     * This is now the real-time BUY *and* SELL trigger: any open
     * position is re-checked against the freshest price/EV on every
     * tick, then a BUY is attempted if no position is open.
     */
    public void onChainlinkPriceUpdate() {

        try {
            evaluateSell();
        } catch (Exception e) {
            log.error(
                    "Error during immediate Chainlink SELL evaluation",
                    e
            );
        }

        try {
            evaluateBuyImmediately();
        } catch (Exception e) {
            log.error(
                    "Error during immediate Chainlink BUY evaluation",
                    e
            );
        }
    }

    private void evaluateBuyImmediately() {

        Optional<PolymarketMarketSnapshot> snapshotOpt =
                marketDataProvider.currentSnapshot();

        if (snapshotOpt.isEmpty()) {
            logSkip(
                    "NO_SNAPSHOT",
                    null,
                    "no open Polymarket market snapshot yet"
            );
            return;
        }

        PolymarketMarketSnapshot snapshot =
                snapshotOpt.get();

        if (snapshot.secondsSinceOpen()
                < tradingProperties.minimumSecondsSinceOpen()) {

            logSkip(
                    "TOO_SOON_AFTER_OPEN",
                    snapshot.slug(),
                    "secondsSinceOpen="
                            + snapshot.secondsSinceOpen()
                            + " minimumSecondsSinceOpen="
                            + tradingProperties.minimumSecondsSinceOpen()
            );

            return;
        }

        if (snapshot.secondsUntilClose()
                < MIN_SECONDS_TO_ACT) {

            logSkip(
                    "TOO_CLOSE_TO_CLOSE",
                    snapshot.slug(),
                    "secondsUntilClose="
                            + snapshot.secondsUntilClose()
                            + " minSecondsToAct="
                            + MIN_SECONDS_TO_ACT
            );

            return;
        }

        if (snapshot.secondsSinceOpen()
                < MIN_SECONDS_TO_ACT) {

            logSkip(
                    "TOO_EARLY_TO_BET",
                    snapshot.slug(),
                    "secondsSinceOpen="
                            + snapshot.secondsSinceOpen()
                            + " minSecondsToAct="
                            + MIN_SECONDS_TO_ACT
            );

            return;
        }

        /*
         * Real mode:
         *
         * One position per market.
         *
         * This means once either UP or DOWN FOK fills, we stop
         * submitting further buys for this market.
         */
        if (!tradingProperties.mock()
                && realBetService.hasOpenBetFor(snapshot.slug())) {

            logSkip(
                    "BET_ALREADY_OPEN",
                    snapshot.slug(),
                    "one real position per market already filled"
            );

            return;
        }

        BigDecimal currentLivePrice =
                prices.getPrice(SYMBOL);

        BigDecimal currentTwapPrice =
                prices.getAvg60sPrice(SYMBOL);

        if (currentLivePrice == null
                || currentLivePrice.signum() <= 0) {

            logSkip(
                    "NO_CURRENT_PRICE",
                    snapshot.slug(),
                    "Chainlink live price unavailable"
            );

            return;
        }

        if (currentTwapPrice == null
                || currentTwapPrice.signum() <= 0) {

            logSkip(
                    "NO_TWAP",
                    snapshot.slug(),
                    "Chainlink 60s TWAP unavailable"
            );

            return;
        }

        if (snapshot.strikePriceUsd() == null
                || snapshot.strikePriceUsd().signum() <= 0) {

            logSkip(
                    "NO_STRIKE",
                    snapshot.slug(),
                    "market strike price unavailable"
            );

            return;
        }

        if (!prices.isPriceFresh(SYMBOL)) {

            logSkip(
                    "STALE_CHAINLINK_PRICE",
                    snapshot.slug(),
                    "ageMs="
                            + prices.getPriceAgeMillis(SYMBOL)
            );

            return;
        }

        double currentUpPrice =
                snapshot.upPrice() != null
                        ? snapshot.upPrice().doubleValue()
                        : 0.5;

        double currentDownPrice =
                snapshot.downPrice() != null
                        ? snapshot.downPrice().doubleValue()
                        : 0.5;

        /*
         * We still use the CURRENT market prices when estimating
         * the current EV for logging/comparison.
         *
         * But we DO NOT use them as the buy order price.
         */
        EvEstimate estimate =
                tradingEngine.estimateUpDown(
                        currentLivePrice,
                        currentTwapPrice,
                        snapshot.strikePriceUsd(),
                        (int) snapshot.secondsUntilClose(),
                        currentUpPrice,
                        currentDownPrice,
                        tradingProperties.takerFee()
                );

        double upThreshold =
                getEffectiveEvThreshold(
                        estimate.upChance()
                );

        double downThreshold =
                getEffectiveEvThreshold(
                        estimate.downChance()
                );

        double upMaxPrice =
                tradingEngine.maxBuyPriceForEv(
                        estimate.upChance(),
                        upThreshold,
                        tradingProperties.takerFee()
                );

        double downMaxPrice =
                tradingEngine.maxBuyPriceForEv(
                        estimate.downChance(),
                        downThreshold,
                        tradingProperties.takerFee()
                );

        log.info(
                "INSTANT BUY EVALUATION slug={} secondsLeft={} live={} twap={} strike={} UP chance={} currentPrice={} threshold={} maxBuy={} DOWN chance={} currentPrice={} threshold={} maxBuy={}",
                snapshot.slug(),
                snapshot.secondsUntilClose(),
                currentLivePrice,
                currentTwapPrice,
                snapshot.strikePriceUsd(),
                round(estimate.upChance()),
                round(currentUpPrice),
                round(upThreshold),
                round(upMaxPrice),
                round(estimate.downChance()),
                round(currentDownPrice),
                round(downThreshold),
                round(downMaxPrice)
        );

        /*
         * MOCK
         *
         * Keep the existing mock strategy behavior for now.
         */
        if (tradingProperties.mock()) {

            MarketSide side =
                    estimate.recommendedSide();

            double chance =
                    estimate.recommendedChance();

            double threshold =
                    getEffectiveEvThreshold(chance);

            if (chance < tradingProperties.minimumWinChance()) {
                return;
            }

            if (estimate.recommendedEv() < threshold) {
                return;
            }

            double marketPrice =
                    side == MarketSide.UP
                            ? currentUpPrice
                            : currentDownPrice;

            List<MockBet> placed =
                    mockBetService.placeBetsForAllStrategies(
                            snapshot,
                            side,
                            estimate.recommendedEv(),
                            chance,
                            BigDecimal.valueOf(marketPrice),
                            currentLivePrice,
                            snapshot.strikePriceUsd(),
                            snapshot.secondsUntilClose()
                    );

            for (MockBet bet : placed) {

                log.info(
                        "DECISION action=BET_PLACED (MOCK) strategy={} slug={} side={} amount={} winChance={} ev={} threshold={}",
                        bet.strategyId(),
                        snapshot.slug(),
                        side,
                        tradingProperties.betAmount(),
                        round(chance),
                        round(estimate.recommendedEv()),
                        round(threshold)
                );
            }

            return;
        }

        /*
         * REAL MODE
         *
         * Try UP and DOWN independently.
         *
         * IMPORTANT:
         *
         * We do NOT require the current ask to already be <= max price.
         *
         * We simply submit a FOK limit order at max price.
         *
         * If the book can immediately fill the whole order at that
         * price or better -> filled.
         *
         * Otherwise -> FOK disappears.
         */

        if (estimate.upChance()
                >= tradingProperties.minimumWinChance()
                && upMaxPrice > 0.0) {

            try {

                RealBet bet =
                        realBetService.placeRealBetAtPrice(
                                snapshot,
                                MarketSide.UP,
                                BigDecimal.valueOf(upMaxPrice),
                                estimate.upEv(),
                                estimate.upChance()
                        );

                log.info(
                        "DECISION action=FOK_FILLED slug={} side=UP maxPrice={} winChance={} threshold={}",
                        snapshot.slug(),
                        upMaxPrice,
                        round(estimate.upChance()),
                        round(upThreshold)
                );

                /*
                 * Once UP fills, do not try DOWN.
                 *
                 * We have one position per market.
                 */
                return;

            } catch (RealBetService.FokNotFilledException e) {

                log.debug(
                        "DECISION action=FOK_NOT_FILLED slug={} side=UP maxPrice={} winChance={}",
                        snapshot.slug(),
                        upMaxPrice,
                        round(estimate.upChance())
                );

            } catch (Exception e) {

                log.error(
                        "DECISION action=FOK_FAILED slug={} side=UP",
                        snapshot.slug(),
                        e
                );
            }
        }

        /*
         * Re-check because another thread/event could potentially
         * have filled a position between the UP and DOWN attempts.
         */
        if (realBetService.hasOpenBetFor(snapshot.slug())) {
            return;
        }

        if (estimate.downChance()
                >= tradingProperties.minimumWinChance()
                && downMaxPrice > 0.0) {

            try {

                RealBet bet =
                        realBetService.placeRealBetAtPrice(
                                snapshot,
                                MarketSide.DOWN,
                                BigDecimal.valueOf(downMaxPrice),
                                estimate.downEv(),
                                estimate.downChance()
                        );

                log.info(
                        "DECISION action=FOK_FILLED slug={} side=DOWN maxPrice={} winChance={} threshold={}",
                        snapshot.slug(),
                        downMaxPrice,
                        round(estimate.downChance()),
                        round(downThreshold)
                );

            } catch (RealBetService.FokNotFilledException e) {

                log.debug(
                        "DECISION action=FOK_NOT_FILLED slug={} side=DOWN maxPrice={} winChance={}",
                        snapshot.slug(),
                        downMaxPrice,
                        round(estimate.downChance())
                );

            } catch (Exception e) {

                log.error(
                        "DECISION action=FOK_FAILED slug={} side=DOWN",
                        snapshot.slug(),
                        e
                );
            }
        }
    }

    /**
     * Runs on the 1-second scheduler as a backup: settles any mock
     * bets whose resolution time has passed, then re-checks SELL as a
     * safety net in case a price tick was missed.
     */
    private void periodicTick() {

        try {
            mockBetService.settleDueBets();
        } catch (Exception e) {
            log.error(
                    "Error during mock bet settlement sweep",
                    e
            );
        }

        try {
            evaluateSell();
        } catch (Exception e) {
            log.error(
                    "Error during periodic SELL evaluation",
                    e
            );
        }
    }

    /**
     * SELL check. Called on every Chainlink price tick (the primary
     * trigger) and again on the 1-second scheduler (backup, in case a
     * tick is missed or the stream stalls).
     *
     * Cheap/idempotent to call twice in the same instant: it only
     * acts when a position is actually open and sellingEv clears the
     * threshold.
     */
    private void evaluateSell() {

        Optional<PolymarketMarketSnapshot> snapshotOpt =
                marketDataProvider.currentSnapshot();

        if (snapshotOpt.isEmpty()) {
            return;
        }

        PolymarketMarketSnapshot snapshot =
                snapshotOpt.get();

        if (tradingProperties.mock()) {

            List<MockBet> soldBets =
                    mockBetService
                            .evaluateSellForAllStrategies(snapshot);

            for (MockBet bet : soldBets) {

                log.info(
                        "DECISION action=SOLD (MOCK) strategy={} slug={} side={} boughtAt={} soldAt={} profitLoss={}",
                        bet.strategyId(),
                        bet.marketSlug(),
                        bet.side(),
                        bet.marketPriceAtBet(),
                        bet.priceAtResolution(),
                        bet.profitLoss()
                );
            }

        } else {

            if (!realBetService.hasOpenBetFor(snapshot.slug())) {
                return;
            }

            Optional<RealBet> sold =
                    realBetService.sellOpenPosition(
                            snapshot
                    );

            sold.ifPresent(
                    bet -> log.info(
                            "DECISION action=SOLD (REAL) slug={} side={} boughtAt={} soldAt={} profitLoss={}",
                            bet.marketSlug(),
                            bet.side(),
                            bet.price(),
                            bet.soldPrice(),
                            bet.profitLoss()
                    )
            );
        }
    }

    private double getEffectiveEvThreshold(
            double winChance) {

        double minEv =
                tradingProperties.minimumExpectedEv();

        double minWinChance =
                tradingProperties.minimumWinChance();

        if (winChance <= minWinChance) {
            return minEv;
        }

        if (winChance >= 0.90) {
            return minEv / 5.0;
        }

        double progress =
                (winChance - minWinChance)
                        / (0.90 - minWinChance);

        return minEv
                * (1.0 - progress * 0.8);
    }

    private void logSkip(
            String reason,
            String slug,
            String detail) {

        String key =
                reason + "|" + slug;

        String previousKey =
                lastSkipKey.getAndSet(key);

        boolean changed =
                !key.equals(previousKey);

        boolean heartbeatDue =
                Duration.between(
                                lastSkipHeartbeatAt.get(),
                                Instant.now()
                        )
                        .compareTo(
                                SKIP_HEARTBEAT_INTERVAL
                        ) >= 0;

        if (changed || heartbeatDue) {

            if (heartbeatDue) {
                lastSkipHeartbeatAt.set(
                        Instant.now()
                );
            }

            log.info(
                    "DECISION skip reason={} slug={} detail='{}'{}",
                    reason,
                    slug,
                    detail,
                    changed
                            ? ""
                            : " (heartbeat, state unchanged)"
            );

        } else {

            log.debug(
                    "DECISION skip reason={} slug={} detail='{}'",
                    reason,
                    slug,
                    detail
            );
        }
    }

    private double round(double value) {
        return Math.round(value * 10000.0)
                / 10000.0;
    }
}