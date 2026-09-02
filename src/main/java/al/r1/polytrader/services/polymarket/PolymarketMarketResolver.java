package al.r1.polytrader.services.polymarket;

import java.util.Optional;

public interface PolymarketMarketResolver {

    Optional<ResolvedMarket> resolveCurrentMarket();

    record ResolvedMarket(String slug, String upTokenId, String downTokenId) {}
}