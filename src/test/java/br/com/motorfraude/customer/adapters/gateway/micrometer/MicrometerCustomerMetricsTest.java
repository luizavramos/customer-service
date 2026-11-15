package br.com.motorfraude.customer.adapters.gateway.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MicrometerCustomerMetricsTest {

    private static final Instant NOW = Instant.parse("2026-11-14T12:00:10Z");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerCustomerMetrics metrics = new MicrometerCustomerMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("conta os eventos do CDC por tabela e operação")
    void eventos() {
        metrics.cdcEvent("customers", "u");
        metrics.cdcEvent("customers", "u");
        metrics.cdcEvent("customer_devices", "c");
        metrics.cdcEvent("customers", "invalid");

        assertThat(registry.counter("customer.cdc.events", "table", "customers", "op", "u").count()).isEqualTo(2);
        assertThat(registry.counter("customer.cdc.events", "table", "customer_devices", "op", "c").count()).isEqualTo(1);
        assertThat(registry.counter("customer.cdc.events", "table", "customers", "op", "invalid").count()).isEqualTo(1);
    }

    @Test
    @DisplayName("conta perfis e tombstones publicados e mede o atraso desde o evento do CDC")
    void publicacao() {
        metrics.profilePublished(Instant.parse("2026-11-14T12:00:07Z"));
        metrics.tombstonePublished();

        assertThat(registry.counter("customer.profiles.published", "kind", "profile").count()).isEqualTo(1);
        assertThat(registry.counter("customer.profiles.published", "kind", "tombstone").count()).isEqualTo(1);
        var lag = registry.timer("customer.cdc.lag");
        assertThat(lag.count()).isEqualTo(1);
        assertThat(lag.totalTime(TimeUnit.SECONDS)).isEqualTo(3.0);
    }

    @Test
    @DisplayName("evento do futuro (relógios diferentes) não gera atraso negativo")
    void semAtrasoNegativo() {
        metrics.profilePublished(NOW.plusSeconds(60));

        assertThat(registry.timer("customer.cdc.lag").totalTime(TimeUnit.SECONDS)).isZero();
    }
}
