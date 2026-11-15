package br.com.motorfraude.customer.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class CustomerServiceIT {

    private static final Network NETWORK = Network.newNetwork();
    private static final String ACCOUNT = "acc_tok_it000001";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16.16")
            .withNetwork(NETWORK).withNetworkAliases("bank-db")
            .withDatabaseName("bank").withUsername("bank").withPassword("bank")
            .withInitScript("bank-schema.sql")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withNetwork(NETWORK).withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> CONNECT = new GenericContainer<>("quay.io/debezium/connect:3.6.3.Final")
            .withNetwork(NETWORK)
            .withExposedPorts(8083)
            .withEnv("BOOTSTRAP_SERVERS", "kafka:9093")
            .withEnv("GROUP_ID", "connect-it")
            .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
            .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
            .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
            .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)));

    private static final String CONNECTOR = """
            {"connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1",
             "database.hostname":"bank-db","database.port":"5432","database.user":"bank","database.password":"bank","database.dbname":"bank",
             "topic.prefix":"cdc","plugin.name":"pgoutput","slot.name":"customers_cdc_slot","publication.name":"dbz_customers",
             "publication.autocreate.mode":"disabled","table.include.list":"bank.customers,bank.customer_devices",
             "snapshot.mode":"initial","tombstones.on.delete":"true",
             "key.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":"false",
             "value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false"}
            """;

    private static void sql(String statement) throws Exception {
        try (Connection c = DriverManager.getConnection(POSTGRES.getJdbcUrl(), "bank", "bank"); var s = c.createStatement()) {
            s.execute(statement);
        }
    }

    private static String connectUrl(String path) {
        return "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083) + path;
    }

    private static void registerConnector() throws Exception {
        var client = HttpClient.newHttpClient();
        var put = HttpRequest.newBuilder(URI.create(connectUrl("/connectors/customers-cdc/config")))
                .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(CONNECTOR)).build();
        var response = client.send(put, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isIn(200, 201);
        for (int i = 0; i < 60; i++) {
            var status = client.send(HttpRequest.newBuilder(URI.create(connectUrl("/connectors/customers-cdc/status"))).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            if (status.contains("\"state\":\"RUNNING\"") && status.contains("\"tasks\":[{")) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new AssertionError("conector não chegou a RUNNING");
    }

    private static final class ProfileReader implements AutoCloseable {
        private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

        ProfileReader() {
            consumer.subscribe(List.of(TopologyFactory.PROFILE_TOPIC));
        }

        ConsumerRecord<String, String> awaitMatch(Predicate<ConsumerRecord<String, String>> match, Duration timeout) {
            var deadline = System.nanoTime() + timeout.toNanos();
            int from = 0;
            while (System.nanoTime() < deadline) {
                for (; from < seen.size(); from++) {
                    if (match.test(seen.get(from))) {
                        return seen.get(from++);
                    }
                }
                consumer.poll(Duration.ofMillis(100)).forEach(seen::add);
            }
            throw new AssertionError("nenhum perfil esperado em " + timeout + "; vistos: " + seen.stream().map(r -> r.key() + "=" + r.value()).toList());
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    private static Predicate<ConsumerRecord<String, String>> profile(Predicate<CustomerProfile> condition) {
        return r -> ACCOUNT.equals(r.key()) && r.value() != null && condition.test(JsonCodec.fromJson(r.value(), CustomerProfile.class));
    }

    @Test
    @DisplayName("do banco ao tópico: snapshot, UPDATE em até 5 s (SC-001), novo dispositivo e remoção com tombstone")
    void ponta_a_ponta(@TempDir Path tmp) throws Exception {
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TopologyFactory.PROFILE_TOPIC, 1, (short) 1))).all().get();
        }
        sql("INSERT INTO bank.customers VALUES ('" + ACCOUNT + "','cus_tok_it000001','Fulano de Tal','111.222.333-96','fulano@exemplo.invalid',"
                + "'+5511999990001','PF','2026-03-01T10:00:00Z',NULL)");
        sql("INSERT INTO bank.customer_devices VALUES ('" + ACCOUNT + "','dev_it0000000001','2026-04-01T10:00:00Z'),"
                + "('" + ACCOUNT + "','dev_it0000000002','2026-05-01T10:00:00Z')");
        registerConnector();

        var settings = new StreamsSettings("customer-service-it", KAFKA.getBootstrapServers(), tmp, 1, 0, "customer-service-it-0", 0);
        try (var app = StreamsAppBootstrap.create(settings, CustomerServiceApp::topology);
                var reader = new ProfileReader()) {
            app.start();

            var snapshot = reader.awaitMatch(profile(p -> p.knownDeviceHashes().size() == 2), Duration.ofSeconds(90));
            ContractAssertions.assertProduces("customer-profile.v1", snapshot.value());
            assertThat(JsonCodec.fromJson(snapshot.value(), CustomerProfile.class).knownDeviceHashes())
                    .containsExactly("dev_it0000000002", "dev_it0000000001");
            assertThat(snapshot.value()).doesNotContain("Fulano", "111.222.333-96", "exemplo.invalid", "5511999990001");

            long start = System.nanoTime();
            sql("UPDATE bank.customers SET segment='PJ', last_credential_change_at='2026-10-05T12:00:00Z' WHERE account_token='" + ACCOUNT + "'");
            var updated = reader.awaitMatch(profile(p -> p.segment() == Segment.PJ), Duration.ofSeconds(30));
            var elapsed = Duration.ofNanos(System.nanoTime() - start);
            ContractAssertions.assertProduces("customer-profile.v1", updated.value());
            assertThat(elapsed).as("SC-001: UPDATE até o perfil publicado").isLessThanOrEqualTo(Duration.ofSeconds(5));

            sql("INSERT INTO bank.customer_devices VALUES ('" + ACCOUNT + "','dev_it0000000003','2026-06-01T10:00:00Z')");
            reader.awaitMatch(profile(p -> p.knownDeviceHashes().size() == 3 && p.knownDeviceHashes().getFirst().equals("dev_it0000000003")),
                    Duration.ofSeconds(30));

            sql("DELETE FROM bank.customer_devices WHERE account_token='" + ACCOUNT + "'");
            sql("DELETE FROM bank.customers WHERE account_token='" + ACCOUNT + "'");
            var tombstone = reader.awaitMatch(r -> ACCOUNT.equals(r.key()) && r.value() == null, Duration.ofSeconds(30));
            assertThat(tombstone.value()).isNull();

            assertThat(app.ready()).isTrue();
            var metrics = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + app.httpPort() + "/actuator/prometheus")).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertThat(metrics).contains("customer_cdc_events_total{op=\"u\",table=\"customers\"", "customer_cdc_events_total{op=\"d\"",
                    "customer_profiles_published_total{kind=\"profile\"", "customer_profiles_published_total{kind=\"tombstone\"",
                    "customer_cdc_lag_seconds_bucket");
        }
    }
}
