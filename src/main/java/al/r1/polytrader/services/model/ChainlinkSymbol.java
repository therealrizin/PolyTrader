package al.r1.polytrader.services.model;

import lombok.Getter;

@Getter
public enum ChainlinkSymbol {
    BTC_USD("btc/usd"),
    ETH_USD("eth/usd"),
    SOL_USD("sol/usd"),
    XRP_USD("xrp/usd");

    private final String wire;

    ChainlinkSymbol(String wire) {
        this.wire = wire;
    }

    public static ChainlinkSymbol fromWire(String wire) {
        if (wire == null) return null;
        String normalized = wire.trim().toLowerCase();
        for (ChainlinkSymbol symbol : values()) {
            if (symbol.wire.equals(normalized)) return symbol;
        }
        return null;
    }

    public static ChainlinkSymbol fromLoose(String value) {
        if (value == null || value.isBlank()) return null;
        String normalized = value.trim().toLowerCase();
        if (!normalized.contains("/")) {
            normalized = normalized + "/usd";
        }
        return fromWire(normalized);
    }
}