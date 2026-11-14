package br.com.motorfraude.customer.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Segment;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopologyFactoryTest {

    private static final String ACCOUNT = "acc_tok_00000001";

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> customers;
    private TestInputTopic<String, String> devices;
    private TestOutputTopic<String, String> profiles;
    private long clock = 1_794_000_000_000L;

    @BeforeEach
    void setUp() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-service-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(TopologyFactory.create(true), props);
        customers = driver.createInputTopic(TopologyFactory.CUSTOMERS_TOPIC, new StringSerializer(), new StringSerializer());
        devices = driver.createInputTopic(TopologyFactory.DEVICES_TOPIC, new StringSerializer(), new StringSerializer());
        profiles = driver.createOutputTopic(TopologyFactory.PROFILE_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String customerKey(String account) {
        return "{\"account_token\":\"" + account + "\"}";
    }

    private String customerEvent(String op, String account, String segment, String credentialChange) {
        var after = "d".equals(op) ? "null" : """
                {"account_token":"%s","customer_token":"cus_tok_x","full_name":"Fulano de Tal","cpf":"111.222.333-96",
                 "email":"fulano@exemplo.invalid","phone":"+5511999990001","segment":"%s",
                 "account_opened_at":"2026-03-01T10:00:00.000000Z","last_credential_change_at":%s}
                """.formatted(account, segment, credentialChange == null ? "null" : "\"" + credentialChange + "\"");
        return "{\"before\":null,\"after\":%s,\"op\":\"%s\",\"ts_ms\":%d}".formatted(after, op, ++clock);
    }

    private void putCustomer(String op, String account, String segment) {
        customers.pipeInput(customerKey(account), customerEvent(op, account, segment, null));
    }

    private void deleteCustomer(String account) {
        customers.pipeInput(customerKey(account), customerEvent("d", account, null, null));
        customers.pipeInput(customerKey(account), (String) null);
    }

    private void putDevice(String account, String hash, String firstSeen) {
        var key = "{\"account_token\":\"%s\",\"device_hash\":\"%s\"}".formatted(account, hash);
        var value = "{\"before\":null,\"after\":{\"account_token\":\"%s\",\"device_hash\":\"%s\",\"first_seen_at\":\"%s\"},\"op\":\"c\",\"ts_ms\":%d}"
                .formatted(account, hash, firstSeen, ++clock);
        devices.pipeInput(key, value);
    }

    private void deleteDevice(String account, String hash) {
        var key = "{\"account_token\":\"%s\",\"device_hash\":\"%s\"}".formatted(account, hash);
        devices.pipeInput(key, "{\"before\":null,\"after\":null,\"op\":\"d\",\"ts_ms\":%d}".formatted(++clock));
        devices.pipeInput(key, (String) null);
    }

    private List<TestRecord<String, String>> drain() {
        var all = new ArrayList<TestRecord<String, String>>();
        while (!profiles.isEmpty()) {
            all.add(profiles.readRecord());
        }
        return all;
    }

    private static CustomerProfile profileOf(TestRecord<String, String> record) {
        ContractAssertions.assertProduces("customer-profile.v1", record.getValue());
        return JsonCodec.fromJson(record.getValue(), CustomerProfile.class);
    }

    private CustomerProfile lastProfile() {
        var all = drain();
        assertThat(all).isNotEmpty();
        return profileOf(all.getLast());
    }

    @Test
    @DisplayName("H1: cliente novo gera o perfil, e uma atualização gera o perfil novo com a mesma chave")
    void atualizacaoGeraPerfil() {
        putCustomer("c", ACCOUNT, "PF");
        var first = drain();
        assertThat(first).hasSize(1);
        assertThat(first.getFirst().getKey()).isEqualTo(ACCOUNT);
        assertThat(profileOf(first.getFirst()).segment()).isEqualTo(Segment.PF);
        assertThat(profileOf(first.getFirst()).knownDeviceHashes()).isEmpty();

        customers.pipeInput(customerKey(ACCOUNT), customerEvent("u", ACCOUNT, "PJ", "2026-10-05T12:00:00.000000Z"));
        var updated = lastProfile();

        assertThat(updated.segment()).isEqualTo(Segment.PJ);
        assertThat(updated.lastCredentialChangeAt()).isEqualTo(Instant.parse("2026-10-05T12:00:00Z"));
        assertThat(updated.updatedAt()).isEqualTo(Instant.ofEpochMilli(clock));
    }

    @Test
    @DisplayName("H1: remover o cliente publica um tombstone (valor nulo) na mesma chave")
    void remocaoGeraTombstone() {
        putCustomer("c", ACCOUNT, "PF");
        drain();

        deleteCustomer(ACCOUNT);
        var all = drain();

        assertThat(all).isNotEmpty();
        assertThat(all.getLast().getKey()).isEqualTo(ACCOUNT);
        assertThat(all.getLast().getValue()).isNull();
    }

    @Test
    @DisplayName("H2: dispositivo que chega antes do cliente não gera perfil; o perfil sai quando o cliente chega")
    void dispositivoAntesDoCliente() {
        putDevice(ACCOUNT, "dev_aaaaaaaa", "2026-04-01T10:00:00.000000Z");
        assertThat(drain()).isEmpty();

        putCustomer("c", ACCOUNT, "PF");

        assertThat(lastProfile().knownDeviceHashes()).containsExactly("dev_aaaaaaaa");
    }

    @Test
    @DisplayName("H2: novo dispositivo atualiza o perfil, com o mais recente primeiro")
    void novoDispositivo() {
        putCustomer("c", ACCOUNT, "PF");
        putDevice(ACCOUNT, "dev_aaaaaaaa", "2026-04-01T10:00:00.000000Z");
        putDevice(ACCOUNT, "dev_bbbbbbbb", "2026-05-01T10:00:00.000000Z");

        var profile = lastProfile();

        assertThat(profile.knownDeviceHashes()).containsExactly("dev_bbbbbbbb", "dev_aaaaaaaa");
        assertThat(profile.updatedAt()).isEqualTo(Instant.ofEpochMilli(clock));
    }

    @Test
    @DisplayName("H2: 15 dispositivos viram os 10 mais recentes por first_seen_at")
    void limiteDeDez() {
        putCustomer("c", ACCOUNT, "PF");
        for (int i = 1; i <= 15; i++) {
            putDevice(ACCOUNT, "dev_%08d".formatted(i), "2026-04-%02dT10:00:00.000000Z".formatted(i));
        }

        var profile = lastProfile();

        assertThat(profile.knownDeviceHashes()).hasSize(10);
        assertThat(profile.knownDeviceHashes().getFirst()).isEqualTo("dev_00000015");
        assertThat(profile.knownDeviceHashes().getLast()).isEqualTo("dev_00000006");
    }

    @Test
    @DisplayName("remover um dispositivo o tira do perfil e traz de volta o 11º")
    void remocaoDeDispositivo() {
        putCustomer("c", ACCOUNT, "PF");
        for (int i = 1; i <= 11; i++) {
            putDevice(ACCOUNT, "dev_%08d".formatted(i), "2026-04-%02dT10:00:00.000000Z".formatted(i));
        }
        drain();

        deleteDevice(ACCOUNT, "dev_00000011");
        var profile = lastProfile();

        assertThat(profile.knownDeviceHashes()).hasSize(10).doesNotContain("dev_00000011").contains("dev_00000001");
    }

    @Test
    @DisplayName("evento repetido gera o mesmo perfil")
    void eventoRepetido() {
        var event = customerEvent("u", ACCOUNT, "PF", null);
        customers.pipeInput(customerKey(ACCOUNT), event);
        var first = lastProfile();

        customers.pipeInput(customerKey(ACCOUNT), event);
        var second = lastProfile();

        assertThat(second).isEqualTo(first);
    }

    @Test
    @DisplayName("contas diferentes não se misturam")
    void contasIndependentes() {
        putCustomer("c", "acc_tok_00000001", "PF");
        putCustomer("c", "acc_tok_00000002", "PJ");
        putDevice("acc_tok_00000002", "dev_bbbbbbbb", "2026-04-01T10:00:00.000000Z");
        var all = drain();

        var ultimoDaConta1 = all.stream().filter(r -> r.getKey().equals("acc_tok_00000001")).reduce((a, b) -> b).orElseThrow();
        var ultimoDaConta2 = all.stream().filter(r -> r.getKey().equals("acc_tok_00000002")).reduce((a, b) -> b).orElseThrow();

        assertThat(profileOf(ultimoDaConta1).knownDeviceHashes()).isEmpty();
        assertThat(profileOf(ultimoDaConta2).knownDeviceHashes()).containsExactly("dev_bbbbbbbb");
    }

    @Test
    @DisplayName("LGPD: nenhum perfil publicado contém nome, cpf, e-mail ou telefone, e todos passam no schema estrito")
    void semDadoPessoal() {
        putCustomer("c", ACCOUNT, "PF");
        putDevice(ACCOUNT, "dev_aaaaaaaa", "2026-04-01T10:00:00.000000Z");
        customers.pipeInput(customerKey(ACCOUNT), customerEvent("u", ACCOUNT, "PJ", null));

        for (var record : drain()) {
            profileOf(record);
            assertThat(record.getValue()).doesNotContain("Fulano", "111.222.333-96", "exemplo.invalid", "+5511999990001", "full_name", "cpf", "email", "phone");
        }
    }
}
