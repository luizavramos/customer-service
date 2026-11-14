package br.com.motorfraude.customer.adapters.gateway.kafka;

import br.com.motorfraude.contracts.CustomerProfile;

public final class ProfileSerde {

    private ProfileSerde() {
    }

    public static JsonSerde<CustomerProfile> create() {
        return JsonSerde.of(CustomerProfile.class);
    }
}
