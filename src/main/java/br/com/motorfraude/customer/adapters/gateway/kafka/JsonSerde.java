package br.com.motorfraude.customer.adapters.gateway.kafka;

import br.com.motorfraude.contracts.JsonCodec;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;

    private JsonSerde(Class<T> type) {
        this.type = type;
    }

    public static <T> JsonSerde<T> of(Class<T> type) {
        return new JsonSerde<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> value == null ? null : JsonCodec.toBytes(value);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> bytes == null ? null : JsonCodec.fromJson(bytes, type);
    }
}
