package al.r1.polytrader.services.betting;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.engine.model.MarketSide;
import al.r1.polytrader.services.betting.model.BetStatus;
import al.r1.polytrader.services.betting.model.MockBet;
import al.r1.polytrader.services.model.ChainlinkSymbol;
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

@Slf4j
@Service
public class MockBetService {

    private static final ChainlinkSymbol SYMBOL = ChainlinkSymbol.BTC_USD;

    private final TradingProperties tradingProperties;

    private final Map<String, MockBet> bets = new ConcurrentHashMap<>();
    private final Set<String> openSlugs = ConcurrentHashMap.newKeySet();

    public boolean hasOpenBetFor(String slug) {
        return openSlugs.contains(slug);
    }

    public MockBetService(TradingProperties tradingProperties) {
        this.tradingProperties = tradingProperties;
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
        Instant resolvesAt = now.plusSeconds(Math.max(secondsUntilClose, 0));

        BigDecimal amount = tradingProperties.mockBetAmount();
        BigDecimal marketPrice = BigDecimal.valueOf(marketPriceAtBet).setScale(4, RoundingMode.HALF_UP);

        BigDecimal grossPayout = amount.divide(marketPrice, 8, RoundingMode.HALF_UP);
        BigDecimal grossProfit = grossPayout.subtract(amount);
        BigDecimal netProfitIfWin = grossProfit.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(tradingProperties.takerFee())))
                .setScale(4, RoundingMode.HALF_UP);

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

        log.info("Placed MOCK bet {} on {} side={} amount={} priceBetAt={} priceToAchieve={} marketPrice={} " +
                        "grossPayout={} grossProfit={} netProfitIfWin={} ev={} winChance={}",
                id, slug, side, amount, priceBetAt, priceToAchieve, marketPrice,
                grossPayout.setScale(4, RoundingMode.HALF_UP),
                grossProfit.setScale(4, RoundingMode.HALF_UP),
                netProfitIfWin,
                countedEv, countedWinChance);

        return bet;
    }

    public void settleDueBets(Prices prices) {
        Instant now = Instant.now();

        for (MockBet bet : bets.values()) {
            if (bet.status() != BetStatus.OPEN) continue;
            if (now.isBefore(bet.resolvesAt())) continue;

            BigDecimal resolutionPrice = prices.getAvg60sPrice(SYMBOL) != null
                    ? prices.getAvg60sPrice(SYMBOL)
                    : prices.getPrice(SYMBOL);

            if (resolutionPrice == null) {
                continue;
            }

            boolean wentUp = resolutionPrice.compareTo(bet.priceToAchieve()) > 0;
            boolean won = (bet.side() == MarketSide.UP) == wentUp;

            BigDecimal grossPayout = bet.amount().divide(bet.marketPriceAtBet(), 8, RoundingMode.HALF_UP);
            BigDecimal grossProfit = grossPayout.subtract(bet.amount());

            BigDecimal profitLoss;
            if (won) {
                profitLoss = grossProfit.multiply(BigDecimal.ONE.subtract(BigDecimal.valueOf(tradingProperties.takerFee())))
                        .setScale(4, RoundingMode.HALF_UP);
            } else {
                profitLoss = bet.amount().negate();
            }

            BetStatus status = won ? BetStatus.WON : BetStatus.LOST;

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
                    bet.potentialValue(),
                    bet.placedAt(),
                    bet.resolvesAt(),
                    status,
                    resolutionPrice,
                    profitLoss
            );

            bets.put(bet.id(), settled);
            openSlugs.remove(bet.marketSlug());

            log.info("Settled MOCK bet {} on {}: {} – resolutionPrice={}, priceToAchieve={}, " +
                            "priceBetAt={}, marketPriceAtBet={}, amount={}, grossPayout={}, grossProfit={}, fee={}, profitLoss={}",
                    bet.id(),
                    bet.marketSlug(),
                    status,
                    resolutionPrice,
                    bet.priceToAchieve(),
                    bet.priceBetAt(),
                    bet.marketPriceAtBet(),
                    bet.amount(),
                    grossPayout.setScale(4, RoundingMode.HALF_UP),
                    grossProfit.setScale(4, RoundingMode.HALF_UP),
                    tradingProperties.takerFee(),
                    profitLoss);
        }
    }

    public List<MockBet> getAllBets() {
        return bets.values().stream()
                .sorted(Comparator.comparing(MockBet::placedAt).reversed())
                .toList();
    }
}