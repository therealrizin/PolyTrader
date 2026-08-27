package al.r1.polytrader.core;

import al.r1.polytrader.services.BackfillService;
import al.r1.polytrader.services.LiveMarketDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TasksRunner implements ApplicationRunner {

    private final BackfillService backfillService;
    private final TaskExecutor taskExecutor;
    private final LiveMarketDataService liveMarketDataService;

    @Override
    public void run(ApplicationArguments args) {
        taskExecutor.execute(() -> {
            try {
                backfillService.gatherAndAnalyze();
                log.info("Backfill complete, starting live data collection");

                liveMarketDataService.start();
            } catch (Exception e) {
                log.error("Backfill failed, live trading will not start", e);
            }
        });
    }
}
