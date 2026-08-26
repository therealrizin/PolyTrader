package al.r1.polytrader.services.binance;

import al.r1.polytrader.services.binance.model.BinanceKline;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

import java.math.BigDecimal;

public class BinanceKlineDeserializer extends ValueDeserializer<BinanceKline> {

    @Override
    public BinanceKline deserialize(
            JsonParser parser,
            DeserializationContext context
    ) {

        JsonNode node = parser.readValueAsTree();

        return new BinanceKline(
                node.get(0).longValue(),
                new BigDecimal(node.get(1).stringValue()),
                new BigDecimal(node.get(2).stringValue()),
                new BigDecimal(node.get(3).stringValue()),
                new BigDecimal(node.get(4).stringValue()),
                new BigDecimal(node.get(5).stringValue()),
                node.get(6).longValue(),
                new BigDecimal(node.get(7).stringValue()),
                node.get(8).longValue(),
                new BigDecimal(node.get(9).stringValue()),
                new BigDecimal(node.get(10).stringValue()),
                node.get(11).stringValue()
        );
    }
}