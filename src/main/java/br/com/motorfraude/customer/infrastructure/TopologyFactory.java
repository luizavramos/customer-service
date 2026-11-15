package br.com.motorfraude.customer.infrastructure;

import br.com.motorfraude.customer.adapters.controller.cdc.CdcFormatException;
import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumCustomerMapper;
import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumDeviceMapper;
import br.com.motorfraude.customer.adapters.gateway.kafka.ProfileSerde;
import br.com.motorfraude.customer.adapters.gateway.kafka.StateSerdes;
import br.com.motorfraude.customer.application.gateway.CustomerMetrics;
import br.com.motorfraude.customer.domain.CustomerRow;
import br.com.motorfraude.customer.domain.DeviceRow;
import br.com.motorfraude.customer.domain.KnownDevices;
import br.com.motorfraude.customer.domain.ProfileProjector;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopologyFactory {

    public static final String CUSTOMERS_TOPIC = "cdc.bank.customers";
    public static final String DEVICES_TOPIC = "cdc.bank.customer_devices";
    public static final String PROFILE_TOPIC = "customer-profile.v1";

    private static final Logger LOG = LoggerFactory.getLogger(TopologyFactory.class);
    private static final String CUSTOMERS = "customers";
    private static final String DEVICES = "customer_devices";

    private TopologyFactory() {
    }

    public static Topology create(CustomerMetrics metrics) {
        return create(metrics, false);
    }

    static Topology create(CustomerMetrics metrics, boolean inMemory) {
        var builder = new StreamsBuilder();

        var customers = builder
                .stream(CUSTOMERS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .flatMap((key, value) -> {
                    try {
                        var change = DebeziumCustomerMapper.parse(value).orElseThrow();
                        var accountId = DebeziumCustomerMapper.accountId(key);
                        metrics.cdcEvent(CUSTOMERS, change.op());
                        return List.of(KeyValue.pair(accountId, change.row()));
                    } catch (CdcFormatException e) {
                        return TopologyFactory.<CustomerRow>invalid(metrics, CUSTOMERS, e);
                    }
                })
                .toTable(Materialized.<String, CustomerRow>as(store("customers", inMemory))
                        .withKeySerde(Serdes.String()).withValueSerde(StateSerdes.customer()));

        var devices = builder
                .stream(DEVICES_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null)
                .flatMap((key, value) -> {
                    try {
                        var change = DebeziumDeviceMapper.parse(value).orElseThrow();
                        var rowKey = DebeziumDeviceMapper.rowKey(key);
                        metrics.cdcEvent(DEVICES, change.op());
                        return List.of(KeyValue.pair(rowKey, change.row()));
                    } catch (CdcFormatException e) {
                        return TopologyFactory.<DeviceRow>invalid(metrics, DEVICES, e);
                    }
                })
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
                .peek((accountId, profile) -> {
                    if (profile == null) {
                        metrics.tombstonePublished();
                    } else {
                        metrics.profilePublished(profile.updatedAt());
                    }
                })
                .to(PROFILE_TOPIC, Produced.with(Serdes.String(), ProfileSerde.create()));

        return builder.build();
    }

    private static <V> List<KeyValue<String, V>> invalid(CustomerMetrics metrics, String table, CdcFormatException error) {
        metrics.cdcEvent(table, "invalid");
        LOG.warn("evento do CDC descartado em {}: {}", table, error.getMessage());
        return List.of();
    }

    private static KeyValueBytesStoreSupplier store(String name, boolean inMemory) {
        return inMemory ? Stores.inMemoryKeyValueStore(name) : Stores.persistentKeyValueStore(name);
    }
}
