package al.r1.polytrader.services.betting;

import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.MockBet;
import al.r1.polytrader.services.model.Prices;
import lombok.extern.slf4j.Slf4j;
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
 * In-memory-only mock betting ledger. No real orders are ever placed here —
 * this exists purely to check whether the algorithm's picks would have won,
 * before any live trading is wired up.
 */
@Slf4j
@Service
public class MockBetService {

    private final Map<String, MockBet> bets = new ConcurrentHashMap<>();
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();

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
            long secondsUntilClose,
            BigDecimal amount,
            double takerFee
    ) {
        String id = UUID.randomUUID().toString();
        BigDecimal marketPrice = BigDecimal.valueOf(marketPriceAtBet).setScale(4, RoundingMode.HALF_UP);

        BigDecimal grossPayout = amount.divide(marketPrice, 8, RoundingMode.HALF_UP);
        BigDecimal grossProfit = grossPayout.subtract(amount);
        BigDecimal potentialValue = grossProfit
                .multiply(BigDecimal.valueOf(1.0 - takerFee))
                .setScale(4, RoundingMode.HALF_UP);

        Instant now = Instant.now();
        MockBet bet = new MockBet(
                id, slug, side, amount, priceBetAt, priceToAchieve, marketPrice,
                countedEv, countedWinChance, potentialValue,
                now, now.plusSeconds(Math.max(secondsUntilClose, 0)),
                BetStatus.OPEN, null, null
        );

        bets.put(id, bet);
        openSlugs.add(slug);

        log.info("Placed MOCK bet {} on {} side={} amount={} priceBetAt={} priceToAchieve={} marketPrice={} ev={} winChance={} potentialValue={}",
                id, slug, side, amount, priceBetAt, priceToAchieve, marketPrice, countedEv, countedWinChance, potentialValue);

        return bet;
    }

    /**
     * Called each tick. Settles any open bet whose window has closed by
     * comparing our current blended price against the reference price it
     * was bet against.
     */
    public void settleDueBets(Prices prices) {
        Instant now = Instant.now();

        for (MockBet bet : bets.values()) {
            if (bet.status() != BetStatus.OPEN) continue;
            if (now.isBefore(bet.resolvesAt())) continue;

            BigDecimal resolutionPrice = prices.getAvg60sPrice() != null
                    ? prices.getAvg60sPrice()
                    : prices.getAvgPrice();

            if (resolutionPrice == null) {
                continue; // wait until we actually have a price to settle against
            }

            boolean wentUp = resolutionPrice.compareTo(bet.priceToAchieve()) > 0;
            boolean won = (bet.side() == MarketSide.UP) == wentUp;

            BetStatus status = won ? BetStatus.WON : BetStatus.LOST;
            BigDecimal profitLoss = won ? bet.potentialValue() : bet.amount().negate();

            MockBet settled = new MockBet(
                    bet.id(), bet.marketSlug(), bet.side(), bet.amount(), bet.priceBetAt(), bet.priceToAchieve(),
                    bet.marketPriceAtBet(), bet.countedEv(), bet.countedWinChance(), bet.potentialValue(),
                    bet.placedAt(), bet.resolvesAt(), status, resolutionPrice, profitLoss
            );

            bets.put(bet.id(), settled);
            openSlugs.remove(bet.marketSlug());

            log.info("Settled MOCK bet {} on {}: {} (resolutionPrice={}, priceToAchieve={}, profitLoss={})",
                    bet.id(), bet.marketSlug(), status, resolutionPrice, bet.priceToAchieve(), profitLoss);
        }
    }

    public List<MockBet> getAllBets() {
        return bets.values().stream()
                .sorted(Comparator.comparing(MockBet::placedAt).reversed())
                .toList();
    }
}