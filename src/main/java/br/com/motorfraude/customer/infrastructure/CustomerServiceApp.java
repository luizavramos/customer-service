package br.com.motorfraude.customer.infrastructure;

import br.com.motorfraude.customer.adapters.gateway.micrometer.MicrometerCustomerMetrics;
import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import org.apache.kafka.streams.Topology;

public final class CustomerServiceApp {

    public static final String APPLICATION_ID = "customer-service";

    private CustomerServiceApp() {
    }

    public static Topology topology(PrometheusMeterRegistry registry) {
        return TopologyFactory.create(new MicrometerCustomerMetrics(registry, Clock.systemUTC()));
    }

    public static void main(String[] args) {
        var settings = StreamsSettings.fromEnvironment(APPLICATION_ID, System.getenv());
        StreamsAppBootstrap.create(settings, CustomerServiceApp::topology).start();
    }
}
