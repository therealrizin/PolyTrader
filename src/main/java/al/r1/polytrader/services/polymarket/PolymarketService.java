package al.r1.polytrader.services.polymarket;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PolymarketService {

    private final PolymarketTwapClient twapClient;

    public void start() {
        twapClient.start();
    }

    public void stop() {
        twapClient.stop();
    }
}