package br.com.motorfraude.customer.adapters.gateway.micrometer;

import br.com.motorfraude.customer.application.gateway.CustomerMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public final class MicrometerCustomerMetrics implements CustomerMetrics {

    private final MeterRegistry registry;
    private final Clock clock;
    private final Timer lag;

    public MicrometerCustomerMetrics(MeterRegistry registry, Clock clock) {
        this.registry = registry;
        this.clock = clock;
        this.lag = Timer.builder("customer.cdc.lag")
                .description("Atraso entre o evento no banco (ts_ms do CDC) e a publicação do perfil")
                .publishPercentileHistogram()
                .register(registry);
        registry.counter("customer.profiles.published", "kind", "profile");
        registry.counter("customer.profiles.published", "kind", "tombstone");
    }

    @Override
    public void cdcEvent(String table, String op) {
        registry.counter("customer.cdc.events", "table", table, "op", op).increment();
    }

    @Override
    public void profilePublished(Instant eventAt) {
        registry.counter("customer.profiles.published", "kind", "profile").increment();
        var delay = Duration.between(eventAt, clock.instant());
        lag.record(delay.isNegative() ? Duration.ZERO : delay);
    }

    @Override
    public void tombstonePublished() {
        registry.counter("customer.profiles.published", "kind", "tombstone").increment();
    }
}
