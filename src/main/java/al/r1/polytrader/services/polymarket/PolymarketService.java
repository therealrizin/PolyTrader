package al.r1.polytrader.services.polymarket;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PolymarketService {

    private final ChainlinkPriceStreamClient chainlinkPriceStreamClient;
    private final PolymarketMarketWebSocketClient clobWebSocketClient;

    public void start() {
        chainlinkPriceStreamClient.start();
        clobWebSocketClient.start();
    }

    public void stop() {
        chainlinkPriceStreamClient.stop();
        clobWebSocketClient.stop();
    }
}