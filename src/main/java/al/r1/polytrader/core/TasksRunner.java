package al.r1.polytrader.core;

import al.r1.polytrader.services.BackfillService;
import al.r1.polytrader.services.LiveMarketDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TasksRunner implements ApplicationRunner {

    private final BackfillService backfillService;
    private final TaskExecutor taskExecutor;
    private final LiveMarketDataService liveMarketDataService;

    public TasksRunner(BackfillService backfillService,
                       @Qualifier("backfillExecutor") TaskExecutor taskExecutor,
                       LiveMarketDataService liveMarketDataService) {
        this.backfillService = backfillService;
        this.taskExecutor = taskExecutor;
        this.liveMarketDataService = liveMarketDataService;
    }

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
