package al.r1.polytrader.api;

import al.r1.polytrader.api.model.BetsResponse;
import al.r1.polytrader.services.betting.MockBetService;
import al.r1.polytrader.services.betting.model.MockBet;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/v1/bets")
public class BetsControllerV1 {

    private final MockBetService mockBetService;

    public BetsControllerV1(MockBetService mockBetService) {
        this.mockBetService = mockBetService;
    }

    @GetMapping
    public BetsResponse getBets() {
        List<MockBet> bets = mockBetService.getAllBets();

        int open = 0;
        int won = 0;
        int lost = 0;
        BigDecimal netProfitLoss = BigDecimal.ZERO;

        for (MockBet bet : bets) {
            switch (bet.status()) {
                case OPEN -> open++;
                case WON -> {
                    won++;
                    netProfitLoss = netProfitLoss.add(bet.profitLoss());
                }
                case LOST -> {
                    lost++;
                    netProfitLoss = netProfitLoss.add(bet.profitLoss());
                }
            }
        }

        return new BetsResponse(bets.size(), open, won, lost, netProfitLoss, bets);
    }
}