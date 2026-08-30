package al.r1.polytrader.services.polymarket;

import al.r1.polytrader.services.polymarket.model.PolymarketMarketSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class PolymarketDataProvider {

    public Optional<PolymarketMarketSnapshot> currentSnapshot() {
        log.debug("MockPolymarketMarketDataProvider has no real market data wired yet; returning empty");
        return Optional.empty();
    }
}