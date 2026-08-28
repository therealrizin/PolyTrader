package al.r1.polytrader.services.polymarket;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PolymarketService {

    private final PolymarketWebSocketClient polymarketWebSocketClient;

    public void start() {
        polymarketWebSocketClient.start();
    }

    public void stop() {
        polymarketWebSocketClient.stop();
    }
}