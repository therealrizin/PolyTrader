package al.r1.polytrader.core;

import al.r1.polytrader.services.BackfillService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class HistoricalDataCollector implements ApplicationRunner {

    private final BackfillService backfillService;
    private final TaskExecutor backfillExecutor;

    @Override
    public void run(ApplicationArguments args) {
        backfillExecutor.execute(backfillService::gatherAndAnalyze);
    }
}
