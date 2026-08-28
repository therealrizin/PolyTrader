package al.r1.polytrader.services.polymarket.model;

public record GammaMarketDto(
        String slug,
        String conditionId,
        String clobTokenIds,
        String resolutionSource,
        Boolean active,
        Boolean closed,
        Boolean acceptingOrders
) {}
