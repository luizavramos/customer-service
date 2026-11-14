package br.com.motorfraude.customer.domain;

import br.com.motorfraude.contracts.CustomerProfile;
import java.time.Instant;

public final class ProfileProjector {

    private ProfileProjector() {
    }

    public static CustomerProfile project(CustomerRow customer, KnownDevices devices) {
        if (customer == null) {
            throw new IllegalArgumentException("sem cliente não há perfil");
        }
        var known = devices == null ? KnownDevices.empty() : devices;
        Instant updatedAt = known.lastEventAt().filter(customer.eventAt()::isBefore).orElse(customer.eventAt());
        return new CustomerProfile(customer.accountId(), customer.segment(), customer.accountOpenedAt(),
                customer.lastCredentialChangeAt(), known.hashes(), updatedAt);
    }
}
