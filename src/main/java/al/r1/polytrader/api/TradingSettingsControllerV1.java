package al.r1.polytrader.api;

import al.r1.polytrader.config.model.TradingProperties;
import al.r1.polytrader.services.RuntimeTradingSettings;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@RestController
@RequestMapping("/api/v1/trading/settings")
public class TradingSettingsControllerV1 {
    private final RuntimeTradingSettings settings;
    private final TradingProperties properties;

    public TradingSettingsControllerV1(RuntimeTradingSettings settings, TradingProperties properties) {
        this.settings = settings;
        this.properties = properties;
    }

    @GetMapping
    public RuntimeTradingSettings.Settings get() { return settings.get(); }

    @PatchMapping
    public RuntimeTradingSettings.Settings update(@RequestHeader(value = "X-Trading-Admin-Token", required = false) String token,
                                                   @RequestBody RuntimeTradingSettings.Update update) {
        if (!authorized(token)) throw new ResponseStatusException(HttpStatus.FORBIDDEN, "valid X-Trading-Admin-Token is required");
        return settings.update(update);
    }

    private boolean authorized(String supplied) {
        String expected = properties.adminToken();
        return expected != null && !expected.isBlank() && supplied != null
                && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), supplied.getBytes(StandardCharsets.UTF_8));
    }
}
