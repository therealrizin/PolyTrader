package al.r1.polytrader.api;

import al.r1.polytrader.api.model.StrategiesResponse;
import al.r1.polytrader.api.model.StrategySummary;
import al.r1.polytrader.services.betting.MockBetService;
import al.r1.polytrader.services.betting.model.MockBet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/bets")
public class BetsControllerV1 {

    private final MockBetService mockBetService;

    public BetsControllerV1(MockBetService mockBetService) {
        this.mockBetService = mockBetService;
    }

    @GetMapping
    public StrategiesResponse getBets() {
        List<MockBet> allBets = mockBetService.getAllBets();

        // Group by strategy
        Map<String, List<MockBet>> betsByStrategy = allBets.stream()
                .collect(Collectors.groupingBy(MockBet::strategyId));

        List<StrategySummary> summaries = new ArrayList<>();
        int totalBets = 0;
        int totalOpen = 0;
        int totalWon = 0;
        int totalLost = 0;
        int totalSold = 0;
        BigDecimal totalNet = BigDecimal.ZERO;

        for (MockBetService.StrategyConfig strategy : mockBetService.getStrategies()) {
            List<MockBet> bets = betsByStrategy.getOrDefault(strategy.id(), List.of());
            int open = 0, won = 0, lost = 0, sold = 0;
            BigDecimal net = BigDecimal.ZERO;

            for (MockBet bet : bets) {
                switch (bet.status()) {
                    case OPEN -> open++;
                    case WON -> { won++; net = net.add(bet.profitLoss()); }
                    case LOST -> { lost++; net = net.add(bet.profitLoss()); }
                    case SOLD -> { sold++; net = net.add(bet.profitLoss()); }
                }
            }

            StrategySummary summary = new StrategySummary(
                    strategy.id(),
                    strategy.minEv(),
                    strategy.minWinChance(),
                    bets.size(),
                    open,
                    won,
                    lost,
                    sold,
                    net
            );
            summaries.add(summary);

            totalBets += bets.size();
            totalOpen += open;
            totalWon += won;
            totalLost += lost;
            totalSold += sold;
            totalNet = totalNet.add(net);
        }

        return new StrategiesResponse(summaries, totalBets, totalOpen, totalWon, totalLost, totalSold, totalNet);
    }
}