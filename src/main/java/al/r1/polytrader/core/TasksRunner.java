package al.r1.polytrader.core;

import al.r1.polytrader.services.LiveMarketDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TasksRunner implements ApplicationRunner {

    private final LiveMarketDataService liveMarketDataService;

    public TasksRunner(LiveMarketDataService liveMarketDataService) {
        this.liveMarketDataService = liveMarketDataService;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("No historical backfill available for free (Chainlink Data Streams REST requires paid/sponsored " +
                "credentials; Polymarket RTDS sends no backlog on subscribe) — starting live data collection directly.");
        liveMarketDataService.start();
    }
}