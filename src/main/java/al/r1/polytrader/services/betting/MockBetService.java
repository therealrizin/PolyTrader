package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class MockBetService {

    private static final ChainlinkSymbol SYMBOL =
            ChainlinkSymbol.BTC_USD;

    /*
     * Don't resolve against an observation too far away from the actual
     * market close.
     *
     * For example, if RTDS temporarily stops sending data for 40 seconds,
     * we don't want to pretend that a 40-second-old price was the official
     * close.
     */
    private static final long MAX_RESOLUTION_DATA_DELAY_MILLIS = 10_000L;

    private final TradingProperties tradingProperties;
    private final Prices prices;

    private final Map<String, MockBet> bets =
            new ConcurrentHashMap<>();

    private final Set<String> openSlugs =
            ConcurrentHashMap.newKeySet();

    public MockBetService(
            TradingProperties tradingProperties,
            Prices prices
    ) {
        this.tradingProperties = tradingProperties;
        this.prices = prices;
    }

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public MockBet placeMockBet(
            String slug,
            MarketSide side,
            BigDecimal priceBetAt,
            BigDecimal priceToAchieve,
            double marketPriceAtBet,
            double countedEv,
            double countedWinChance,
            long secondsUntilClose
    ) {
        String id = UUID.randomUUID().toString();

        Instant now = Instant.now();

        Instant resolvesAt = now.plusSeconds(
                Math.max(secondsUntilClose, 0)
        );

        BigDecimal amount =
                tradingProperties.betAmount();

        BigDecimal marketPrice =
                BigDecimal.valueOf(marketPriceAtBet)
                        .setScale(4, RoundingMode.HALF_UP);

        BigDecimal grossPayout =
                amount.divide(
                        marketPrice,
                        8,
                        RoundingMode.HALF_UP
                );

        BigDecimal grossProfit =
                grossPayout.subtract(amount);

        BigDecimal netProfitIfWin =
                grossProfit
                        .multiply(
                                BigDecimal.ONE.subtract(
                                        BigDecimal.valueOf(
                                                tradingProperties.takerFee()
                                        )
                                )
                        )
                        .setScale(
                                4,
                                RoundingMode.HALF_UP
                        );

        MockBet bet = new MockBet(
                id,
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
        openSlugs.add(slug);

        log.info(
                "Placed MOCK bet {} on {} side={} amount={} " +
                        "priceBetAt={} priceToAchieve={} marketPrice={} " +
                        "grossPayout={} grossProfit={} netProfitIfWin={} " +
                        "ev={} winChance={} resolvesAt={}",
                id,
                slug,
                side,
                amount,
                priceBetAt,
                priceToAchieve,
                marketPrice,
                grossPayout.setScale(4, RoundingMode.HALF_UP),
                grossProfit.setScale(4, RoundingMode.HALF_UP),
                netProfitIfWin,
                countedEv,
                countedWinChance,
                resolvesAt
        );

        return bet;
    }

    /**
     * Checks whether the currently open MOCK bet (if any) for the market
     * in {@code snapshot} should be sold back to the market right now,
     * instead of held to resolution.
     *
     * Unlike the buy decision, win chance plays NO role here: selling
     * realizes a KNOWN, certain outcome at the current bid, so the only
     * question is whether that certain outcome clears the same EV bar
     * used for entries ({@code trading.minimum-expected-ev}).
     *
     * On a successful sell, the slug's slot is freed immediately
     * (removed from openSlugs) so a fresh entry can be taken in the very
     * same market window on the next evaluation, if a new edge appears.
     *
     * @return the resulting SOLD bet, if a sell was executed
     */
    public Optional<MockBet> maybeSellOpenPosition(
            PolymarketMarketSnapshot snapshot
    ) {
        if (snapshot == null) {
            return Optional.empty();
        }

        String slug = snapshot.slug();

        if (slug == null || !openSlugs.contains(slug)) {
            return Optional.empty();
        }

        for (MockBet bet : bets.values()) {

            if (bet.status() != BetStatus.OPEN) {
                continue;
            }

            if (!slug.equals(bet.marketSlug())) {
                continue;
            }

            BigDecimal currentBid =
                    bet.side() == MarketSide.UP
                            ? snapshot.upBid()
                            : snapshot.downBid();

            if (currentBid == null || currentBid.signum() <= 0) {
                log.debug(
                        "Cannot evaluate sell for MOCK bet {} on {}: no live bid for side={}",
                        bet.id(), slug, bet.side()
                );
                continue;
            }

            BigDecimal netProfitIfSold = netProfitFromSelling(bet, currentBid);

            double sellingEv =
                    netProfitIfSold
                            .divide(bet.amount(), 8, RoundingMode.HALF_UP)
                            .doubleValue();

            if (sellingEv >= tradingProperties.minimumExpectedEv()) {
                return Optional.of(sellBet(bet, currentBid, netProfitIfSold, sellingEv));
            } else {
                // Promote from DEBUG to INFO for better visibility
                log.info(
                        "SELL_CHECK slug={} betId={} side={} currentBid={} sellingEv={} threshold={} -> hold",
                        slug, bet.id(), bet.side(), currentBid, sellingEv,
                        tradingProperties.minimumExpectedEv()
                );
            }
        }

        return Optional.empty();
    }

    /**
     * Net profit (already fee-adjusted) if the bet were closed right now
     * at {@code currentBid}. The taker fee only applies to positive
     * profit, matching the settlement logic in {@link #settleBet}: a
     * losing close isn't further reduced by fees, it's just the shares'
     * mark-to-market loss.
     */
    private BigDecimal netProfitFromSelling(
            MockBet bet,
            BigDecimal currentBid
    ) {
        BigDecimal shares =
                bet.amount().divide(
                        bet.marketPriceAtBet(),
                        8,
                        RoundingMode.HALF_UP
                );

        BigDecimal grossProceeds =
                shares.multiply(currentBid);

        BigDecimal grossProfit =
                grossProceeds.subtract(bet.amount());

        if (grossProfit.signum() <= 0) {
            return grossProfit.setScale(4, RoundingMode.HALF_UP);
        }

        return grossProfit
                .multiply(
                        BigDecimal.ONE.subtract(
                                BigDecimal.valueOf(tradingProperties.takerFee())
                        )
                )
                .setScale(4, RoundingMode.HALF_UP);
    }

    private MockBet sellBet(
            MockBet bet,
            BigDecimal currentBid,
            BigDecimal netProfit,
            double sellingEv
    ) {
        MockBet sold = new MockBet(
                bet.id(),
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

        // Free the slot: this is what allows re-entry into the same
        // window on the very next evaluation.
        openSlugs.remove(bet.marketSlug());

        log.info(
                "Sold MOCK bet {} on {}: side={} boughtAt={} sellingBid={} " +
                        "amount={} netProfit={} sellingEv={} threshold={} (slot freed for re-entry)",
                bet.id(),
                bet.marketSlug(),
                bet.side(),
                bet.marketPriceAtBet(),
                currentBid,
                bet.amount(),
                netProfit,
                sellingEv,
                tradingProperties.minimumExpectedEv()
        );

        return sold;
    }

    /**
     * Settles bets using the timestamped raw Chainlink data received
     * directly from Polymarket RTDS.
     *
     * IMPORTANT:
     *
     * We do NOT use prices.getPrice() here.
     *
     * getPrice() represents "latest now", which can already be after
     * the market has closed.
     */
    public void settleDueBets() {
        Instant now = Instant.now();

        for (MockBet bet : bets.values()) {

            if (bet.status() != BetStatus.OPEN) {
                continue;
            }

            if (now.isBefore(bet.resolvesAt())) {
                continue;
            }

            settleBet(bet);
        }
    }

    private void settleBet(MockBet bet) {

        long targetCloseMillis =
                bet.resolvesAt().toEpochMilli();

        Prices.PriceObservation resolution =
                prices.getRawPriceAtOrBefore(
                        SYMBOL,
                        targetCloseMillis
                );

        if (resolution == null) {

            log.warn(
                    "Cannot settle MOCK bet {}: " +
                            "no Polymarket Chainlink observation at/before " +
                            "close time {}",
                    bet.id(),
                    bet.resolvesAt()
            );

            /*
             * DO NOT mark the bet LOST.
             *
             * Missing market data is not a market outcome.
             */
            return;
        }

        long dataDelay =
                targetCloseMillis -
                        resolution.observedAtMillis();

        if (dataDelay > MAX_RESOLUTION_DATA_DELAY_MILLIS) {

            log.warn(
                    "Cannot safely settle MOCK bet {}: " +
                            "closest Polymarket Chainlink observation is {}ms " +
                            "before market close. close={} observation={}",
                    bet.id(),
                    dataDelay,
                    bet.resolvesAt(),
                    Instant.ofEpochMilli(
                            resolution.observedAtMillis()
                    )
            );

            /*
             * Again: don't invent an outcome when the source data
             * is missing/stale.
             */
            return;
        }

        BigDecimal resolutionPrice =
                resolution.price();

        /*
         * This is the actual market question:
         *
         * UP:
         *     closing Chainlink BTC price > strike
         *
         * DOWN:
         *     closing Chainlink BTC price <= strike
         */
        int comparison =
                resolutionPrice.compareTo(
                        bet.priceToAchieve()
                );

        boolean wentUp = comparison > 0;

        boolean won =
                (bet.side() == MarketSide.UP)
                        == wentUp;

        BigDecimal grossPayout =
                bet.amount().divide(
                        bet.marketPriceAtBet(),
                        8,
                        RoundingMode.HALF_UP
                );

        BigDecimal grossProfit =
                grossPayout.subtract(
                        bet.amount()
                );

        BigDecimal profitLoss;

        if (won) {

            profitLoss =
                    grossProfit
                            .multiply(
                                    BigDecimal.ONE.subtract(
                                            BigDecimal.valueOf(
                                                    tradingProperties
                                                            .takerFee()
                                            )
                                    )
                            )
                            .setScale(
                                    4,
                                    RoundingMode.HALF_UP
                            );

        } else {

            profitLoss =
                    bet.amount().negate();
        }

        BetStatus status =
                won
                        ? BetStatus.WON
                        : BetStatus.LOST;

        MockBet settled =
                new MockBet(
                        bet.id(),
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

        bets.put(
                bet.id(),
                settled
        );

        openSlugs.remove(
                bet.marketSlug()
        );

        log.info(
                "Settled MOCK bet {} on {}: {} | " +
                        "resolutionPrice={} | strike={} | " +
                        "side={} | observationTime={} | " +
                        "delay={}ms | priceBetAt={} | " +
                        "marketPriceAtBet={} | amount={} | " +
                        "grossPayout={} | grossProfit={} | " +
                        "fee={} | profitLoss={}",
                bet.id(),
                bet.marketSlug(),
                status,
                resolutionPrice,
                bet.priceToAchieve(),
                bet.side(),
                Instant.ofEpochMilli(
                        resolution.observedAtMillis()
                ),
                dataDelay,
                bet.priceBetAt(),
                bet.marketPriceAtBet(),
                bet.amount(),
                grossPayout.setScale(
                        4,
                        RoundingMode.HALF_UP
                ),
                grossProfit.setScale(
                        4,
                        RoundingMode.HALF_UP
                ),
                tradingProperties.takerFee(),
                profitLoss
        );
    }

    public List<MockBet> getAllBets() {
        return bets.values()
                .stream()
                .sorted(
                        Comparator.comparing(
                                MockBet::placedAt
                        ).reversed()
                )
                .toList();
    }
}