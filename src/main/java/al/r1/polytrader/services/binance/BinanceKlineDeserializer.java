package al.r1.polytrader.services.binance;

import al.r1.polytrader.services.binance.model.BinanceKline;
import org.springframework.boot.json.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.math.BigDecimal;

public class BinanceKlineDeserializer
        extends JsonDeserializer<BinanceKline> {

    @Override
    public BinanceKline deserialize(
            JsonParser parser,
            DeserializationContext context
    ) throws IOException {

        JsonNode node = parser.getCodec().readTree(parser);

        return new BinanceKline(
                node.get(0).asLong(),
                new BigDecimal(node.get(1).asText()),
                new BigDecimal(node.get(2).asText()),
                new BigDecimal(node.get(3).asText()),
                new BigDecimal(node.get(4).asText()),
                new BigDecimal(node.get(5).asText()),
                node.get(6).asLong(),
                new BigDecimal(node.get(7).asText()),
                node.get(8).asLong(),
                new BigDecimal(node.get(9).asText()),
                new BigDecimal(node.get(10).asText()),
                node.get(11).asText()
        );
    }
}