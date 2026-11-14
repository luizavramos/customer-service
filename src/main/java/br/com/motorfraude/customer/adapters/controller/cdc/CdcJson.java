package br.com.motorfraude.customer.adapters.controller.cdc;

import br.com.motorfraude.contracts.JsonCodec;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

final class CdcJson {

    private static final Set<String> OPERATIONS = Set.of("c", "u", "r", "d");

    private CdcJson() {
    }

    static JsonNode tree(String json) {
        try {
            return JsonCodec.readTree(json);
        } catch (JacksonException | IllegalArgumentException e) {
            throw new CdcFormatException("JSON do CDC inválido", e);
        }
    }

    static String operation(JsonNode event) {
        var op = event.path("op").asString("");
        if (!OPERATIONS.contains(op)) {
            throw new CdcFormatException("op desconhecida no evento do CDC: '" + op + "'");
        }
        return op;
    }

    static Instant eventAt(JsonNode event) {
        var ts = event.path("ts_ms");
        if (!ts.isNumber()) {
            throw new CdcFormatException("campo ts_ms ausente no evento do CDC");
        }
        return Instant.ofEpochMilli(ts.asLong());
    }

    static JsonNode after(JsonNode event) {
        var after = event.path("after");
        if (!after.isObject()) {
            throw new CdcFormatException("campo after ausente no evento do CDC");
        }
        return after;
    }

    static String requiredText(JsonNode row, String field) {
        var value = row.path(field);
        if (!value.isString() || value.asString().isBlank()) {
            throw new CdcFormatException("campo " + field + " ausente no evento do CDC");
        }
        return value.asString();
    }

    static Instant requiredInstant(JsonNode row, String field) {
        return parseInstant(field, requiredText(row, field));
    }

    static Instant optionalInstant(JsonNode row, String field) {
        var value = row.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        return parseInstant(field, requiredText(row, field));
    }

    private static Instant parseInstant(String field, String text) {
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            throw new CdcFormatException("campo " + field + " com data inválida: " + text, e);
        }
    }
}
