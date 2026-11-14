package br.com.motorfraude.customer.domain;

import java.time.Instant;
import java.util.Objects;

public record DeviceRow(String accountId, String deviceHash, Instant firstSeenAt, Instant eventAt) {

    public DeviceRow {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(deviceHash, "deviceHash");
        Objects.requireNonNull(firstSeenAt, "firstSeenAt");
        Objects.requireNonNull(eventAt, "eventAt");
    }
}
