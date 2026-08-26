package al.r1.polytrader.services;

public enum CurrencyPairs {
    BTCUSD("BTCUSD"), ETHUSD("ETHUSD");

    private final String value;

    CurrencyPairs(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
