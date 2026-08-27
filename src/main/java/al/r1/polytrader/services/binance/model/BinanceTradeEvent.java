package al.r1.polytrader.services.binance.model;

public record BinanceTradeEvent(
        String e,
        long E,
        String s,
        long t,
        String p,//price
        String q,
        long T,
        boolean m
) {}