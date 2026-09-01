package al.r1.polytrader.services.betting;

import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.MockBet;
import al.r1.polytrader.services.model.Prices;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In‑memory mock betting ledger. Uses fixed stake from config and
 * calculates realised profit/loss at settlement, applying taker fee
 * only to profits (not to the principal).
 */
@Slf4j
@Service
public class MockBetService {

    @Value("${trading.taker-fee:0.07}")
    private double takerFee;

    @Value("${trading.mock-bet-amount:1.0}")
    private BigDecimal mockBetAmount;

    private final Map<String, MockBet> bets = new ConcurrentHashMap<>();
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    /**
     * Places a mock bet with the fixed stake from configuration.
     * All profit/loss calculations are deferred to settlement.
     */
    public MockBet placeMockBet(
            String slug,
            MarketSide side,
            BigDecimal priceBetAt,          // the odds at which we bought
            BigDecimal priceToAchieve,      // reference price (strike) for outcome
            double marketPriceAtBet,
            double countedEv,
            double countedWinChance,
            long secondsUntilClose
    ) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        Instant resolvesAt = now.plusSeconds(Math.max(secondsUntilClose, 0));

        BigDecimal amount = mockBetAmount;

        // Pre‑compute potential profit if the bet wins (for logging and the record's potentialValue)
        BigDecimal grossPayout = amount.divide(priceBetAt, 8, RoundingMode.HALF_UP);
        BigDecimal grossProfit = grossPayout.subtract(amount);
        BigDecimal netProfitIfWin = grossProfit.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(takerFee)))
                .setScale(4, RoundingMode.HALF_UP);

        MockBet bet = new MockBet(
                id,
                slug,
                side,
                amount,
                priceBetAt,                     // corresponds to record's priceBetAt
                priceToAchieve,                 // record's priceToAchieve
                BigDecimal.valueOf(marketPriceAtBet),
                countedEv,
                countedWinChance,
                netProfitIfWin,                 // potentialValue (net profit if win)
                now,
                resolvesAt,
                BetStatus.OPEN,
                null,
                null
        );

        bets.put(id, bet);
        openSlugs.add(slug);

        log.info("Placed MOCK bet {} on {} side={} amount={} priceBetAt={} priceToAchieve={} " +
                        "grossPayout={} grossProfit={} netProfitIfWin={} ev={} winChance={}",
                id, slug, side, amount, priceBetAt, priceToAchieve,
                grossPayout.setScale(4, RoundingMode.HALF_UP),
                grossProfit.setScale(4, RoundingMode.HALF_UP),
                netProfitIfWin,
                countedEv, countedWinChance);

        return bet;
    }

    /**
     * Called periodically. Settles any open bet whose window has closed.
     * Actual profit/loss is computed from the stored priceBetAt and the resolution price.
     */
    public void settleDueBets(Prices prices) {
        Instant now = Instant.now();

        for (MockBet bet : bets.values()) {
            if (bet.status() != BetStatus.OPEN) continue;
            if (now.isBefore(bet.resolvesAt())) continue;

            // Use best available price to resolve the bet
            BigDecimal resolutionPrice = prices.getAvg60sPrice() != null
                    ? prices.getAvg60sPrice()
                    : prices.getAvgPrice();

            if (resolutionPrice == null) {
                continue; // wait until we have a price
            }

            // Determine win/loss: went up if resolutionPrice > priceToAchieve
            boolean wentUp = resolutionPrice.compareTo(bet.priceToAchieve()) > 0;
            boolean won = (bet.side() == MarketSide.UP) == wentUp;

            // Compute actual P&L from the stored bet parameters
            BigDecimal grossPayout = bet.amount().divide(bet.priceBetAt(), 8, RoundingMode.HALF_UP);
            BigDecimal grossProfit = grossPayout.subtract(bet.amount());

            BigDecimal profitLoss;
            if (won) {
                // Fee is applied to gross profit
                profitLoss = grossProfit.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(takerFee)))
                        .setScale(4, RoundingMode.HALF_UP);
            } else {
                profitLoss = bet.amount().negate();
            }

            BetStatus status = won ? BetStatus.WON : BetStatus.LOST;

            // Create a new immutable bet record with updated status and P&L
            MockBet settled = new MockBet(
                    bet.id(),
                    bet.marketSlug(),
                    bet.side(),
                    bet.amount(),
                    bet.priceBetAt(),
                    bet.priceToAchieve(),
                    bet.marketPriceAtBet(),
                    bet.countedEv(),
                    bet.countedWinChance(),
                    bet.potentialValue(),   // keep the original potential value for reference
                    bet.placedAt(),
                    bet.resolvesAt(),
                    status,
                    resolutionPrice,
                    profitLoss
            );

            bets.put(bet.id(), settled);
            openSlugs.remove(bet.marketSlug());

            log.info("Settled MOCK bet {} on {}: {} – resolutionPrice={}, priceToAchieve={}, " +
                            "priceBetAt={}, amount={}, grossPayout={}, grossProfit={}, fee={}, profitLoss={}",
                    bet.id(),
                    bet.marketSlug(),
                    status,
                    resolutionPrice,
                    bet.priceToAchieve(),
                    bet.priceBetAt(),
                    bet.amount(),
                    grossPayout.setScale(4, RoundingMode.HALF_UP),
                    grossProfit.setScale(4, RoundingMode.HALF_UP),
                    takerFee,
                    profitLoss);
        }
    }

    public List<MockBet> getAllBets() {
        return bets.values().stream()
                .sorted(Comparator.comparing(MockBet::placedAt).reversed())
                .toList();
    }
}