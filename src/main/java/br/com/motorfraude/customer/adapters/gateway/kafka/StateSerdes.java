package br.com.motorfraude.customer.adapters.gateway.kafka;

import br.com.motorfraude.customer.domain.CustomerRow;
import br.com.motorfraude.customer.domain.DeviceRow;
import br.com.motorfraude.customer.domain.KnownDevices;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class StateSerdes {

    private StateSerdes() {
    }

    public static Serde<CustomerRow> customer() {
        return JsonSerde.of(CustomerRow.class);
    }

    public static Serde<DeviceRow> device() {
        return JsonSerde.of(DeviceRow.class);
    }

    public static Serde<KnownDevices> knownDevices() {
        var rows = JsonSerde.of(DeviceRow[].class);
        return new Serde<>() {
            @Override
            public Serializer<KnownDevices> serializer() {
                return (topic, value) -> value == null ? null : rows.serializer().serialize(topic, value.rows().toArray(DeviceRow[]::new));
            }

            @Override
            public Deserializer<KnownDevices> deserializer() {
                return (topic, bytes) -> {
                    if (bytes == null) {
                        return null;
                    }
                    return KnownDevices.of(List.of(rows.deserializer().deserialize(topic, bytes)));
                };
            }
        };
    }
}
