package br.com.motorfraude.customer.domain;

import br.com.motorfraude.contracts.Segment;
import java.time.Instant;
import java.util.Objects;

public record CustomerRow(String accountId, Segment segment, Instant accountOpenedAt, Instant lastCredentialChangeAt, Instant eventAt) {

    public CustomerRow {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(segment, "segment");
        Objects.requireNonNull(accountOpenedAt, "accountOpenedAt");
        Objects.requireNonNull(eventAt, "eventAt");
    }
}
