package br.com.motorfraude.customer.adapters.controller.cdc;

import br.com.motorfraude.customer.domain.DeviceRow;
import java.util.Optional;

public final class DebeziumDeviceMapper {

    private static final char SEPARATOR = '|';

    public record DeviceChange(String op, DeviceRow row) {
    }

    private DebeziumDeviceMapper() {
    }

    public static String rowKey(String keyJson) {
        var key = CdcJson.tree(keyJson);
        return CdcJson.requiredText(key, "account_token") + SEPARATOR + CdcJson.requiredText(key, "device_hash");
    }

    public static String accountIdOf(String rowKey) {
        return rowKey.substring(0, rowKey.indexOf(SEPARATOR));
    }

    public static Optional<DeviceChange> parse(String valueJson) {
        if (valueJson == null || valueJson.isBlank()) {
            return Optional.empty();
        }
        var event = CdcJson.tree(valueJson);
        var op = CdcJson.operation(event);
        var eventAt = CdcJson.eventAt(event);
        if ("d".equals(op)) {
            return Optional.of(new DeviceChange(op, null));
        }
        var after = CdcJson.after(event);
        var row = new DeviceRow(
                CdcJson.requiredText(after, "account_token"),
                CdcJson.requiredText(after, "device_hash"),
                CdcJson.requiredInstant(after, "first_seen_at"),
                eventAt);
        return Optional.of(new DeviceChange(op, row));
    }
}
