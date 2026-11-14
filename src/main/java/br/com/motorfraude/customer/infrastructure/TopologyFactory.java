package br.com.motorfraude.customer.infrastructure;

import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumCustomerMapper;
import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumDeviceMapper;
import br.com.motorfraude.customer.adapters.gateway.kafka.ProfileSerde;
import br.com.motorfraude.customer.adapters.gateway.kafka.StateSerdes;
import br.com.motorfraude.customer.domain.CustomerRow;
import br.com.motorfraude.customer.domain.DeviceRow;
import br.com.motorfraude.customer.domain.KnownDevices;
import br.com.motorfraude.customer.domain.ProfileProjector;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public final class TopologyFactory {

    public static final String CUSTOMERS_TOPIC = "cdc.bank.customers";
    public static final String DEVICES_TOPIC = "cdc.bank.customer_devices";
    public static final String PROFILE_TOPIC = "customer-profile.v1";

    private static KeyValueBytesStoreSupplier store(String name, boolean inMemory) {
        return inMemory ? Stores.inMemoryKeyValueStore(name) : Stores.persistentKeyValueStore(name);
    }

    private TopologyFactory() {
    }

    public static Topology create() {
        return create(false);
    }

    static Topology create(boolean inMemory) {
        var builder = new StreamsBuilder();

        var customers = builder
                .stream(CUSTOMERS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .map((key, value) -> KeyValue.pair(
                        DebeziumCustomerMapper.accountId(key),
                        DebeziumCustomerMapper.parse(value).map(DebeziumCustomerMapper.CustomerChange::row).orElse(null)))
                .toTable(Materialized.<String, CustomerRow>as(store("customers", inMemory))
                        .withKeySerde(Serdes.String()).withValueSerde(StateSerdes.customer()));

        var devices = builder
                .stream(DEVICES_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .map((key, value) -> KeyValue.pair(
                        DebeziumDeviceMapper.rowKey(key),
                        DebeziumDeviceMapper.parse(value).map(DebeziumDeviceMapper.DeviceChange::row).orElse(null)))
                .toTable(Materialized.<String, DeviceRow>as(store("devices", inMemory))
                        .withKeySerde(Serdes.String()).withValueSerde(StateSerdes.device()))
                .groupBy((rowKey, row) -> KeyValue.pair(row.accountId(), row), Grouped.with(Serdes.String(), StateSerdes.device()))
                .aggregate(
                        KnownDevices::empty,
                        (accountId, row, known) -> known.add(row),
                        (accountId, row, known) -> known.remove(row),
                        Materialized.<String, KnownDevices>as(store("known-devices", inMemory))
                                .withKeySerde(Serdes.String()).withValueSerde(StateSerdes.knownDevices()));

        customers
                .leftJoin(devices, ProfileProjector::project)
                .toStream()
                .to(PROFILE_TOPIC, Produced.with(Serdes.String(), ProfileSerde.create()));

        return builder.build();
    }
}
