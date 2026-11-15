package br.com.motorfraude.customer.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.commons.logging.PiiPatterns;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Schemas;
import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumCustomerMapper;
import br.com.motorfraude.customer.application.gateway.CustomerMetrics;
import br.com.motorfraude.customer.adapters.controller.cdc.DebeziumDeviceMapper;
import br.com.motorfraude.customer.domain.CustomerRow;
import br.com.motorfraude.customer.domain.KnownDevices;
import br.com.motorfraude.customer.domain.ProfileProjector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PrivacyTest {

    private static final int CUSTOMERS = 10_000;
    private static final int THROUGH_TOPOLOGY = 300;

    @TempDir
    Path stateDir;

    private static String cpf(int i) {
        var digits = "%011d".formatted(i * 7919L % 100_000_000_000L);
        return digits.substring(0, 3) + "." + digits.substring(3, 6) + "." + digits.substring(6, 9) + "-" + digits.substring(9);
    }

    private static String account(int i) {
        return "acc_tok_%08d".formatted(i);
    }

    private static String customerEvent(int i, long ts) {
        var after = """
                {"account_token":"%s","customer_token":"cus_tok_%08d","full_name":"Fulano Sintetico %d","cpf":"%s",
                 "email":"cliente%d@exemplo.invalid","phone":"+55119%08d","segment":"%s",
                 "account_opened_at":"2026-03-07T21:22:46.195757Z","last_credential_change_at":null}
                """.formatted(account(i), i, i, cpf(i), i, i, i % 5 == 0 ? "PJ" : "PF");
        return "{\"before\":null,\"after\":%s,\"op\":\"r\",\"ts_ms\":%d}".formatted(after, ts);
    }

    private static String deviceKey(int i, int d) {
        return "{\"account_token\":\"%s\",\"device_hash\":\"dev_%08d%02d\"}".formatted(account(i), i, d);
    }

    private static String deviceEvent(int i, int d, long ts) {
        return "{\"before\":null,\"after\":{\"account_token\":\"%s\",\"device_hash\":\"dev_%08d%02d\",\"first_seen_at\":\"2026-04-0%dT10:00:00.000000Z\"},\"op\":\"r\",\"ts_ms\":%d}"
                .formatted(account(i), i, d, d + 1, ts);
    }

    private static void assertClean(String account, String profileJson, ArrayList<String> violations) {
        var errors = Schemas.validate("customer-profile.v1", JsonCodec.readTree(profileJson)).errors();
        if (!errors.isEmpty()) {
            violations.add(account + ": " + errors);
        }
        if (!PiiPatterns.mask(profileJson).equals(profileJson) || profileJson.contains("Fulano")) {
            violations.add(account + ": padrão de dado pessoal no perfil");
        }
    }

    @Test
    @DisplayName("SC-002: os perfis de 10 mil clientes sintéticos (mapeadores e projetor) passam no schema e não têm dado pessoal")
    void dezMilClientes() {
        var violations = new ArrayList<String>();
        long ts = 1_794_000_000_000L;
        for (int i = 1; i <= CUSTOMERS; i++) {
            var customer = DebeziumCustomerMapper.parse(customerEvent(i, ++ts)).orElseThrow().row();
            var devices = KnownDevices.empty();
            for (int d = 0; d < 1 + i % 4; d++) {
                devices = devices.add(DebeziumDeviceMapper.parse(deviceEvent(i, d, ++ts)).orElseThrow().row());
            }
            assertClean(account(i), JsonCodec.toJson(ProfileProjector.project(customer, devices)), violations);
        }

        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("SC-002: pela topologia inteira, os perfis publicados e o estado do Kafka Streams não têm dado pessoal")
    void pelaTopologia() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-service-privacy");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        try (var driver = new TopologyTestDriver(TopologyFactory.create(CustomerMetrics.NOOP, true), props)) {
            var customers = driver.createInputTopic(TopologyFactory.CUSTOMERS_TOPIC, new StringSerializer(), new StringSerializer());
            var devices = driver.createInputTopic(TopologyFactory.DEVICES_TOPIC, new StringSerializer(), new StringSerializer());
            var profiles = driver.createOutputTopic(TopologyFactory.PROFILE_TOPIC, new StringDeserializer(), new StringDeserializer());

            long ts = 1_794_000_000_000L;
            for (int i = 1; i <= THROUGH_TOPOLOGY; i++) {
                customers.pipeInput("{\"account_token\":\"" + account(i) + "\"}", customerEvent(i, ++ts));
                for (int d = 0; d < 1 + i % 4; d++) {
                    devices.pipeInput(deviceKey(i, d), deviceEvent(i, d, ++ts));
                }
            }

            var latest = new HashMap<String, String>();
            for (var record : profiles.readRecordsToList()) {
                latest.put(record.getKey(), record.getValue());
            }
            assertThat(latest).hasSize(THROUGH_TOPOLOGY);
            var violations = new ArrayList<String>();
            latest.forEach((account, json) -> assertClean(account, json, violations));
            assertThat(violations).isEmpty();

            var store = driver.<String, CustomerRow>getKeyValueStore("customers");
            try (var all = store.all()) {
                int rows = 0;
                while (all.hasNext()) {
                    var json = JsonCodec.toJson(all.next().value);
                    assertThat(PiiPatterns.mask(json)).isEqualTo(json);
                    assertThat(json).doesNotContain("Fulano", "exemplo.invalid", "cpf", "email", "phone", "full_name");
                    rows++;
                }
                assertThat(rows).isEqualTo(THROUGH_TOPOLOGY);
            }
        }
    }
}
