package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.TradingEngine;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.MockBet;
import al.r1.polytrader.services.model.ChainlinkSymbol;
import al.r1.polytrader.services.model.Prices;
import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class MockBetService {

    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;
    private static final long MAX_RESOLUTION_DATA_DELAY_MILLIS = 10_000L;

    // ----- Strategy definitions -----
    public record StrategyConfig(String id, double minEv, double minWinChance) {}

    public static final List<StrategyConfig> STRATEGIES = List.of(
            new StrategyConfig("option0", 0.15, 0.45),
            new StrategyConfig("option1", 0.25, 0.35),
            new StrategyConfig("option2", 0.35, 0.25),
            new StrategyConfig("option3", 0.50, 0.20),
            new StrategyConfig("option4", 1.0, 0.1),
            new StrategyConfig("option5", 2.0, 0.1)
    );

    // ----- Dependencies -----
    private final TradingProperties tradingProperties;
    private final Prices prices;
    private final TradingEngine tradingEngine;

    // ----- State -----
    private final Map<String, MockBet> bets = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> openSlugsByStrategy = new ConcurrentHashMap<>();

    public MockBetService(TradingProperties tradingProperties, Prices prices, TradingEngine tradingEngine) {
        this.tradingProperties = tradingProperties;
        this.prices = prices;
        this.tradingEngine = tradingEngine;
        for (StrategyConfig s : STRATEGIES) {
            openSlugsByStrategy.put(s.id(), ConcurrentHashMap.newKeySet());
        }
    }

    // ----- Backward‑compatible methods (called by TradingDecisionService) -----

    public boolean hasOpenBetFor(String slug) {
        return openSlugsByStrategy.values().stream().anyMatch(set -> set.contains(slug));
    }

    public Optional<MockBet> maybeSellOpenPosition(PolymarketMarketSnapshot snapshot) {
        List<MockBet> sold = evaluateSellForAllStrategies(snapshot);
        return sold.isEmpty() ? Optional.empty() : Optional.of(sold.get(0));
    }

    public Optional<MockBet> placeMockBet(
            String slug,
            MarketSide side,
            BigDecimal priceBetAt,
            BigDecimal priceToAchieve,
            double marketPriceAtBet,
            double countedEv,
            double countedWinChance,
            long secondsUntilClose
    ) {
        for (StrategyConfig strategy : STRATEGIES) {
            if (countedEv >= strategy.minEv() && countedWinChance >= strategy.minWinChance()) {
                Set<String> openSlugs = openSlugsByStrategy.get(strategy.id());
                if (openSlugs != null && openSlugs.add(slug)) {
                    BigDecimal amount = tradingProperties.betAmount();
                    if (amount == null || amount.signum() <= 0) {
                        openSlugs.remove(slug);
                        return Optional.empty();
                    }
                    String id = UUID.randomUUID().toString();
                    Instant now = Instant.now();
                    Instant resolvesAt = now.plusSeconds(Math.max(secondsUntilClose, 0));
                    BigDecimal marketPrice = BigDecimal.valueOf(marketPriceAtBet).setScale(8, RoundingMode.HALF_UP);

                    // --- Exact Polymarket fee at entry ---
                    BigDecimal shares = amount.divide(marketPrice, 8, RoundingMode.HALF_UP);
                    BigDecimal grossPayout = shares; // each share pays $1
                    BigDecimal grossProfit = grossPayout.subtract(amount);
                    BigDecimal fee = calculateFee(amount, marketPrice); // amount * (1 - price) * feeRate
                    BigDecimal netProfitIfWin = grossProfit.subtract(fee).setScale(4, RoundingMode.HALF_UP);

                    MockBet bet = new MockBet(
                            id,
                            strategy.id(),
                            slug,
                            side,
                            amount,
                            priceBetAt,
                            priceToAchieve,
                            marketPrice,
                            countedEv,
                            countedWinChance,
                            netProfitIfWin,
                            now,
                            resolvesAt,
                            BetStatus.OPEN,
                            null,
                            null
                    );
                    bets.put(id, bet);
                    log.info("Placed MOCK bet (compat) id={} strategy={} on {} side={} amount={} fee={} netProfitIfWin={}",
                            id, strategy.id(), slug, side, amount, fee, netProfitIfWin);
                    return Optional.of(bet);
                }
            }
        }
        return Optional.empty();
    }

    // ----- New multi‑strategy methods -----

    public List<MockBet> placeBetsForAllStrategies(
            PolymarketMarketSnapshot snapshot,
            MarketSide side,
            double countedEv,
            double countedWinChance,
            BigDecimal marketPriceAtBet,
            BigDecimal priceBetAt,
            BigDecimal priceToAchieve,
            long secondsUntilClose
    ) {
        List<MockBet> placed = new ArrayList<>();
        for (StrategyConfig strategy : STRATEGIES) {
            Optional<MockBet> bet = placeMockBetIfEligible(
                    snapshot.slug(),
                    side,
                    countedEv,
                    countedWinChance,
                    marketPriceAtBet,
                    priceBetAt,
                    priceToAchieve,
                    secondsUntilClose,
                    strategy
            );
            bet.ifPresent(placed::add);
        }
        return placed;
    }

    public List<MockBet> evaluateSellForAllStrategies(PolymarketMarketSnapshot snapshot) {
        List<MockBet> sold = new ArrayList<>();
        if (snapshot == null) return sold;

        String slug = snapshot.slug();
        if (slug == null) return sold;

        for (StrategyConfig strategy : STRATEGIES) {
            Set<String> openSlugs = openSlugsByStrategy.get(strategy.id());
            if (openSlugs == null || !openSlugs.contains(slug)) continue;

            MockBet bet = findOpenBetForStrategy(slug, strategy.id());
            if (bet == null) continue;

            BigDecimal currentBid = bet.side() == MarketSide.UP ? snapshot.upBid() : snapshot.downBid();
            if (currentBid == null || currentBid.signum() <= 0) continue;

            BigDecimal netProfitIfSold = netProfitFromSelling(bet, currentBid);
            double sellingEv = netProfitIfSold.divide(bet.amount(), 8, RoundingMode.HALF_UP).doubleValue();

            if (sellingEv >= strategy.minEv()) {
                MockBet soldBet = sellBet(bet, currentBid, netProfitIfSold, sellingEv);
                sold.add(soldBet);
            } else {
                log.debug("SELL_CHECK (MOCK) strategy={} slug={} betId={} sellingEv={} threshold={} -> hold",
                        strategy.id(), slug, bet.id(), sellingEv, strategy.minEv());
            }
        }
        return sold;
    }

    public void settleDueBets() {
        Instant now = Instant.now();
        for (MockBet bet : bets.values()) {
            if (bet.status() != BetStatus.OPEN) continue;
            if (now.isBefore(bet.resolvesAt())) continue;
            settleBet(bet);
        }
    }

    // ----- Private helpers -----

    /**
     * Polymarket taker fee formula (exactly as in RealBetService.netSellProceeds):
     * fee = amount * (1 - price) * feeRate
     */
    private BigDecimal calculateFee(BigDecimal amount, BigDecimal price) {
        BigDecimal feeRate = BigDecimal.valueOf(tradingProperties.takerFee());
        return amount.multiply(BigDecimal.ONE.subtract(price))
                .multiply(feeRate)
                .setScale(8, RoundingMode.HALF_UP);
    }

    private Optional<MockBet> placeMockBetIfEligible(
            String slug,
            MarketSide side,
            double countedEv,
            double countedWinChance,
            BigDecimal marketPriceAtBet,
            BigDecimal priceBetAt,
            BigDecimal priceToAchieve,
            long secondsUntilClose,
            StrategyConfig strategy
    ) {
        if (countedEv < strategy.minEv() || countedWinChance < strategy.minWinChance()) {
            return Optional.empty();
        }
        Set<String> openSlugs = openSlugsByStrategy.get(strategy.id());
        if (openSlugs == null || !openSlugs.add(slug)) {
            return Optional.empty();
        }
        BigDecimal amount = tradingProperties.betAmount();
        if (amount == null || amount.signum() <= 0) {
            openSlugs.remove(slug);
            return Optional.empty();
        }
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        Instant resolvesAt = now.plusSeconds(Math.max(secondsUntilClose, 0));
        BigDecimal marketPrice = marketPriceAtBet.setScale(8, RoundingMode.HALF_UP);

        // Exact Polymarket fee at entry (taken at the moment of the bet)
        BigDecimal shares = amount.divide(marketPrice, 8, RoundingMode.HALF_UP);
        BigDecimal grossPayout = shares;
        BigDecimal grossProfit = grossPayout.subtract(amount);
        BigDecimal fee = calculateFee(amount, marketPrice);
        BigDecimal netProfitIfWin = grossProfit.subtract(fee).setScale(4, RoundingMode.HALF_UP);

        MockBet bet = new MockBet(
                id,
                strategy.id(),
                slug,
                side,
                amount,
                priceBetAt,
                priceToAchieve,
                marketPrice,
                countedEv,
                countedWinChance,
                netProfitIfWin,
                now,
                resolvesAt,
                BetStatus.OPEN,
                null,
                null
        );
        bets.put(id, bet);
        log.info("Placed MOCK bet id={} strategy={} on {} side={} amount={} price={} fee={} netProfitIfWin={}",
                id, strategy.id(), slug, side, amount, marketPrice, fee, netProfitIfWin);
        return Optional.of(bet);
    }

    private MockBet findOpenBetForStrategy(String slug, String strategyId) {
        for (MockBet bet : bets.values()) {
            if (bet.status() == BetStatus.OPEN &&
                    slug.equals(bet.marketSlug()) &&
                    strategyId.equals(bet.strategyId())) {
                return bet;
            }
        }
        return null;
    }

    private BigDecimal netProfitFromSelling(MockBet bet, BigDecimal currentBid) {
        BigDecimal shares = bet.amount().divide(bet.marketPriceAtBet(), 8, RoundingMode.HALF_UP);
        BigDecimal grossProceeds = shares.multiply(currentBid);
        BigDecimal grossProfit = grossProceeds.subtract(bet.amount());
        if (grossProfit.signum() <= 0) {
            return grossProfit.setScale(4, RoundingMode.HALF_UP);
        }
        // Fee applied on sell proceeds (exact Polymarket formula)
        BigDecimal fee = calculateFee(bet.amount(), currentBid); // amount * (1 - bid) * feeRate
        return grossProfit.subtract(fee).setScale(4, RoundingMode.HALF_UP);
    }

    private MockBet sellBet(MockBet bet, BigDecimal currentBid, BigDecimal netProfit, double sellingEv) {
        MockBet sold = new MockBet(
                bet.id(),
                bet.strategyId(),
                bet.marketSlug(),
                bet.side(),
                bet.amount(),
                bet.priceBetAt(),
                bet.priceToAchieve(),
                bet.marketPriceAtBet(),
                bet.countedEv(),
                bet.countedWinChance(),
                bet.potentialValue(),
                bet.placedAt(),
                bet.resolvesAt(),
                BetStatus.SOLD,
                currentBid,
                netProfit
        );
        bets.put(bet.id(), sold);
        Set<String> openSlugs = openSlugsByStrategy.get(bet.strategyId());
        if (openSlugs != null) openSlugs.remove(bet.marketSlug());
        log.info("Sold MOCK bet id={} strategy={} on {}: netProfit={} sellingEv={}",
                bet.id(), bet.strategyId(), bet.marketSlug(), netProfit, sellingEv);
        return sold;
    }

    private void settleBet(MockBet bet) {
        long targetCloseMillis = bet.resolvesAt().toEpochMilli();
        Prices.PriceObservation resolution = prices.getRawPriceAtOrBefore(SYMBOL, targetCloseMillis);
        if (resolution == null || (targetCloseMillis - resolution.observedAtMillis()) > MAX_RESOLUTION_DATA_DELAY_MILLIS) {
            log.warn("Cannot settle MOCK bet {} (strategy {}): data missing/stale", bet.id(), bet.strategyId());
            return;
        }
        BigDecimal resolutionPrice = resolution.price();
        int comparison = resolutionPrice.compareTo(bet.priceToAchieve());
        boolean wentUp = comparison > 0;
        boolean won = (bet.side() == MarketSide.UP) == wentUp;

        BigDecimal shares = bet.amount().divide(bet.marketPriceAtBet(), 8, RoundingMode.HALF_UP);
        BigDecimal grossPayout = shares;
        BigDecimal grossProfit = grossPayout.subtract(bet.amount());
        BigDecimal profitLoss;
        if (won) {
            // Fee already paid at entry – we only subtract it from the gross profit
            BigDecimal fee = calculateFee(bet.amount(), bet.marketPriceAtBet());
            profitLoss = grossProfit.subtract(fee).setScale(4, RoundingMode.HALF_UP);
        } else {
            profitLoss = bet.amount().negate();
        }

        BetStatus status = won ? BetStatus.WON : BetStatus.LOST;
        MockBet settled = new MockBet(
                bet.id(),
                bet.strategyId(),
                bet.marketSlug(),
                bet.side(),
                bet.amount(),
                bet.priceBetAt(),
                bet.priceToAchieve(),
                bet.marketPriceAtBet(),
                bet.countedEv(),
                bet.countedWinChance(),
                bet.potentialValue(),
                bet.placedAt(),
                bet.resolvesAt(),
                status,
                resolutionPrice,
                profitLoss
        );
        bets.put(bet.id(), settled);
        Set<String> openSlugs = openSlugsByStrategy.get(bet.strategyId());
        if (openSlugs != null) openSlugs.remove(bet.marketSlug());
        log.info("Settled MOCK bet id={} strategy={} on {}: {} profitLoss={}",
                bet.id(), bet.strategyId(), bet.marketSlug(), status, profitLoss);
    }

    // ----- Query methods for API -----
    public List<MockBet> getAllBets() {
        return bets.values().stream()
                .sorted(Comparator.comparing(MockBet::placedAt).reversed())
                .toList();
    }

    public List<StrategyConfig> getStrategies() {
        return STRATEGIES;
    }
}