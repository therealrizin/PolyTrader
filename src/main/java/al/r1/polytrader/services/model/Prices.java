package al.r1.polytrader.services.model;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class Prices {

    private static final int HISTORY_SIZE = 60;

    private record SymbolState(
            AtomicReference<BigDecimal> price,
            AtomicReference<BigDecimal> avg60sPrice,
            Deque<PricePoint> history
    ) {
        static SymbolState empty() {
            return new SymbolState(new AtomicReference<>(), new AtomicReference<>(), new ArrayDeque<>());
        }
    }

    private final Map<ChainlinkSymbol, SymbolState> states = new EnumMap<>(ChainlinkSymbol.class);

    public Prices() {
        for (ChainlinkSymbol symbol : ChainlinkSymbol.values()) {
            states.put(symbol, SymbolState.empty());
        }
    }

    public void updatePrice(ChainlinkSymbol symbol, BigDecimal price) {
        if (symbol == null || price == null) return;
        states.get(symbol).price().set(price);
    }

    public void updateAvg60sPrice(ChainlinkSymbol symbol, BigDecimal avg60sPrice) {
        if (symbol == null || avg60sPrice == null) return;
        states.get(symbol).avg60sPrice().set(avg60sPrice);
    }

    public BigDecimal getPrice(ChainlinkSymbol symbol) {
        return states.get(symbol).price().get();
    }

    public BigDecimal getAvg60sPrice(ChainlinkSymbol symbol) {
        return states.get(symbol).avg60sPrice().get();
    }

    public synchronized void recordSnapshot(ChainlinkSymbol symbol, LocalDateTime at) {
        SymbolState state = states.get(symbol);
        state.history().addLast(new PricePoint(at, state.price().get(), state.avg60sPrice().get()));
        while (state.history().size() > HISTORY_SIZE) {
            state.history().pollFirst();
        }
    }

    public synchronized List<PricePoint> getRecentHistory(ChainlinkSymbol symbol) {
        return new ArrayList<>(states.get(symbol).history());
    }
}