package br.com.motorfraude.customer.adapters.controller.cdc;

import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.customer.domain.CustomerRow;
import java.util.Optional;

public final class DebeziumCustomerMapper {

    public record CustomerChange(String op, CustomerRow row) {
    }

    private DebeziumCustomerMapper() {
    }

    public static String accountId(String keyJson) {
        return CdcJson.requiredText(CdcJson.tree(keyJson), "account_token");
    }

    public static Optional<CustomerChange> parse(String valueJson) {
        if (valueJson == null || valueJson.isBlank()) {
            return Optional.empty();
        }
        var event = CdcJson.tree(valueJson);
        var op = CdcJson.operation(event);
        var eventAt = CdcJson.eventAt(event);
        if ("d".equals(op)) {
            return Optional.of(new CustomerChange(op, null));
        }
        var after = CdcJson.after(event);
        var segment = CdcJson.requiredText(after, "segment");
        var row = new CustomerRow(
                CdcJson.requiredText(after, "account_token"),
                parseSegment(segment),
                CdcJson.requiredInstant(after, "account_opened_at"),
                CdcJson.optionalInstant(after, "last_credential_change_at"),
                eventAt);
        return Optional.of(new CustomerChange(op, row));
    }

    private static Segment parseSegment(String text) {
        try {
            return Segment.valueOf(text);
        } catch (IllegalArgumentException e) {
            throw new CdcFormatException("campo segment com valor inválido: " + text, e);
        }
    }
}
