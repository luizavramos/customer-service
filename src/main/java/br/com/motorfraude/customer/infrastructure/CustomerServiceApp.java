package br.com.motorfraude.customer.infrastructure;

import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;

public final class CustomerServiceApp {

    public static final String APPLICATION_ID = "customer-service";

    private CustomerServiceApp() {
    }

    public static void main(String[] args) {
        var settings = StreamsSettings.fromEnvironment(APPLICATION_ID, System.getenv());
        StreamsAppBootstrap.create(settings, TopologyFactory.create()).start();
    }
}
