package al.r1.polytrader.services;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.ProbabilityTable;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.EvEstimate;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.BetService;
import al.r1.polytrader.services.betting.model.Bet;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.PolymarketDataProvider;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class TradingDecisionService {
    private static final int MIN_SECONDS_TO_ACT = 10;
    private static final Duration SKIP_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);

    private final Prices prices;
    private final TradingEngine tradingEngine;
    private final PolymarketDataProvider marketDataProvider;
    private final BetService betService;
    private final TradingProperties tradingProperties;

    private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();
    private final AtomicBoolean tradingActive = new AtomicBoolean(false);
    private final AtomicReference<String> lastSkipKey = new AtomicReference<>();
    private final AtomicReference<Instant> lastSkipHeartbeatAt = new AtomicReference<>(Instant.EPOCH);
    private final Deque<Integer> binanceTicks = new ArrayDeque<>(3);
    private BigDecimal previousBinancePrice;

    public TradingDecisionService(Prices prices, TradingEngine tradingEngine,
                                  PolymarketDataProvider marketDataProvider,
                                  BetService betService, TradingProperties tradingProperties) {
        this.prices = prices;
        this.tradingEngine = tradingEngine;
        this.marketDataProvider = marketDataProvider;
        this.betService = betService;
        this.tradingProperties = tradingProperties;
    }

    public void start() {
        tradingActive.set(true);
        log.info("Trading loop started (mock={}, minEv={}, minWinChance={}, takerFee={}, minSecondsSinceOpen={})",
                tradingProperties.mock(), tradingProperties.minimumExpectedEv(),
                tradingProperties.minimumWinChance(), tradingProperties.takerFee(),
                tradingProperties.minimumSecondsSinceOpen());
    }

    public void stop() {
        tradingActive.set(false);
    }

    public synchronized void onBinancePriceUpdate(BigDecimal binancePrice) {
        if (!tradingActive.get()) return;
        if (binancePrice == null || binancePrice.signum() <= 0) return;

        BigDecimal previousPrice = previousBinancePrice;
        previousBinancePrice = binancePrice;
        if (previousPrice == null || previousPrice.signum() <= 0) return;

        int tick = binancePrice.compareTo(previousPrice);
        binanceTicks.addLast(tick);
        while (binanceTicks.size() > 3) binanceTicks.pollFirst();
        if (binanceTicks.size() < 3) return;

        ChainlinkSymbol symbol = ChainlinkSymbol.BTC_USD;
        BigDecimal polymarketPrice = prices.getPrice(symbol);
        BigDecimal polymarketTwap = prices.getAvg60sPrice(symbol);
        if (polymarketPrice == null || polymarketTwap == null) return;

        BigDecimal projectedPolymarketPrice = polymarketPrice.multiply(
                binancePrice.divide(previousPrice, MathContext.DECIMAL64), MathContext.DECIMAL64);
        int trendLayer = ProbabilityTable.trendLayer(
                binanceTicks.peekFirst(),
                binanceTicks.stream().skip(1).findFirst().orElse(0),
                binanceTicks.peekLast());
        if (!tradingEngine.hasProbabilityData(trendLayer)) {
            logSkip("NO_TREND_PROBABILITY_DATA", null, "trendLayer=" + trendLayer);
            return;
        }

        try { evaluateBuy(symbol, projectedPolymarketPrice, polymarketTwap, trendLayer); }
        catch (Exception e) { log.error("Error during BUY evaluation", e); }
        try { evaluateSell(symbol); } catch (Exception e) { log.error("Error during SELL evaluation", e); }
    }

    private void evaluateBuy(ChainlinkSymbol symbol, BigDecimal projectedLivePrice,
                             BigDecimal currentTwapPrice, int trendLayer) {
        Optional<PolymarketMarketSnapshot> snapshotOpt = marketDataProvider.currentSnapshot();
        if (snapshotOpt.isEmpty()) {
            logSkip("NO_SNAPSHOT", null, "no open Polymarket market snapshot yet");
            return;
        }
        PolymarketMarketSnapshot snapshot = snapshotOpt.get();

        if (!isTimeToBetValid(snapshot)) return;

        BigDecimal currentLivePrice = prices.getPrice(symbol);
        if (!isPriceValidForTrade(snapshot, currentLivePrice, currentTwapPrice, symbol)) return;

        EvEstimate estimate = tradingEngine.estimatePricesToMeetEv(
                projectedLivePrice, currentTwapPrice,
                snapshot.resolutionPrice(), (int) snapshot.secondsUntilClose(), trendLayer);

        log.info("BUY EVALUATION:\n" +
                        "downChance={}\ndownEvRequired={}\ndownPriceToMeetEv={}\n" +
                        "upChance={}\nupEvRequired={}\nupPriceToMeetEv={}",
                estimate.downChance(), estimate.downEvRequired(), estimate.downPriceToMeetEv(),
                estimate.upChance(), estimate.upEvRequired(), estimate.upPriceToMeetEv());

        boolean betUp = placeBet(snapshot, estimate, MarketSide.UP, symbol, projectedLivePrice);
        if (!betUp) placeBet(snapshot, estimate, MarketSide.DOWN, symbol, projectedLivePrice);
    }

    private boolean placeBet(PolymarketMarketSnapshot snapshot, EvEstimate estimate,
                             MarketSide side, ChainlinkSymbol symbol, BigDecimal projectedLivePrice) {
        double chance = side == MarketSide.UP ? estimate.upChance() : estimate.downChance();
        double betPrice = side == MarketSide.UP ? estimate.upPriceToMeetEv() : estimate.downPriceToMeetEv();
        double ev = side == MarketSide.UP ? estimate.upEvRequired() : estimate.downEvRequired();

        if (chance < tradingProperties.minimumWinChance() || betPrice <= 0.0) {
            return betService.hasOpenBetFor(snapshot.slug());
        }

        log.info("BET_DECISION:\nslug={}\nside={}\nwinChance={}\nevAtCurrentPrice={}\nbetPrice={}\nlivePrice={}\ntwapPrice={}\npriceToAchieve={}",
                snapshot.slug(), side, chance, ev, betPrice,
                projectedLivePrice, prices.getAvg60sPrice(symbol), snapshot.resolutionPrice());

        try {
            betService.placeBet(snapshot, side, betPrice, ev, chance, symbol);
            return true;
        } catch (BetService.FokNotFilledException e) {
            log.debug("DECISION action=FOK_NOT_FILLED slug={} side={} betPrice={} winChance={}",
                    snapshot.slug(), side, betPrice, round(chance));
        } catch (Exception e) {
            log.error("DECISION action=FOK_FAILED slug={} side={} reason={}", snapshot.slug(), side, e.getMessage());
        }
        return betService.hasOpenBetFor(snapshot.slug());
    }

    private void evaluateSell(ChainlinkSymbol symbol) {
        marketDataProvider.currentSnapshot()
                .ifPresent(snapshot -> betService.sellOpenPosition(snapshot, symbol));
    }

    private boolean isTimeToBetValid(PolymarketMarketSnapshot snapshot) {
        if (snapshot.secondsSinceOpen() < MIN_SECONDS_TO_ACT || snapshot.secondsUntilClose() < MIN_SECONDS_TO_ACT) {
            logSkip("MIN_SECONDS_TO_ACT", snapshot.slug(), "Minimum seconds to act is lower than " + MIN_SECONDS_TO_ACT);
            return false;
        }
        if (!tradingProperties.mock() && betService.hasOpenBetFor(snapshot.slug())) {
            logSkip("BET_ALREADY_OPEN", snapshot.slug(), "one real position per market already filled");
            return false;
        }
        return true;
    }

    private boolean isPriceValidForTrade(PolymarketMarketSnapshot snapshot, BigDecimal currentLivePrice,
                                         BigDecimal currentTwapPrice, ChainlinkSymbol symbol) {
        if (currentLivePrice == null || currentLivePrice.signum() <= 0 || currentTwapPrice == null || currentTwapPrice.signum() <= 0) {
            logSkip("NO_CURRENT_PRICE", snapshot.slug(), "Chainlink live price unavailable");
            return false;
        }
        if (snapshot.resolutionPrice() == null || snapshot.resolutionPrice().signum() <= 0) {
            logSkip("NO_STRIKE", snapshot.slug(), "market strike price unavailable");
            return false;
        }
        if (!prices.isPriceFresh(symbol)) {
            logSkip("STALE_CHAINLINK_PRICE", snapshot.slug(), "ageMs=" + prices.getPriceAgeMillis(symbol));
            return false;
        }
        return true;
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
