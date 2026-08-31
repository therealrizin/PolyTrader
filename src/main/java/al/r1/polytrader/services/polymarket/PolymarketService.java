package al.r1.polytrader.services.polymarket;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PolymarketService {

    private final PolymarketTwapClient twapClient;
    private final PolymarketMarketWebSocketClient clobWebSocketClient;

    public void start() {
        twapClient.start();
        clobWebSocketClient.start();
    }

    public void stop() {
        twapClient.stop();
        clobWebSocketClient.stop();
    }
}