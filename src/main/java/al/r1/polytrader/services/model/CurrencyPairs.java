package al.r1.polytrader.services.model;

import lombok.Getter;

@Getter
public enum CurrencyPairs {
    BTCUSD("BTCUSD"), BTCUSDT("BTCUSDT"), ETHUSD("ETHUSD"), ETHUSDT("ETHUSDT");

    private final String value;

    CurrencyPairs(String value){
        this.value = value;
    }

    public static CurrencyPairs getUsdXValue(CurrencyPairs pair) {
        return switch (pair) {
            case BTCUSD, BTCUSDT -> BTCUSD;
            case ETHUSD, ETHUSDT -> ETHUSD;
        };
    }
}
