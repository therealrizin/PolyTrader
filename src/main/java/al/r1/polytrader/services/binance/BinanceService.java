package al.r1.polytrader.services;

import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class BinanceService {

    private final WebClient webClient;


    public BinanceService(WebClient webClient) {
        this.webClient = webClient;
    }

    
}
