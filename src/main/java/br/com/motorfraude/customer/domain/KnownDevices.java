package br.com.motorfraude.customer.domain;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class KnownDevices {

    public static final int LIMIT = 10;

    private static final Comparator<DeviceRow> MOST_RECENT_FIRST =
            Comparator.comparing(DeviceRow::firstSeenAt).reversed().thenComparing(DeviceRow::deviceHash);

    private static final KnownDevices EMPTY = new KnownDevices(Map.of());

    private final Map<String, DeviceRow> byHash;

    private KnownDevices(Map<String, DeviceRow> byHash) {
        this.byHash = byHash;
    }

    public static KnownDevices empty() {
        return EMPTY;
    }

    public KnownDevices add(DeviceRow device) {
        var next = new HashMap<>(byHash);
        next.merge(device.deviceHash(), device, (old, incoming) -> incoming.eventAt().isBefore(old.eventAt()) ? old : incoming);
        return new KnownDevices(Map.copyOf(next));
    }

    public KnownDevices remove(DeviceRow device) {
        var next = new HashMap<>(byHash);
        next.remove(device.deviceHash());
        return new KnownDevices(Map.copyOf(next));
    }

    public static KnownDevices of(java.util.Collection<DeviceRow> rows) {
        var result = EMPTY;
        for (var row : rows) {
            result = result.add(row);
        }
        return result;
    }

    public List<DeviceRow> rows() {
        return byHash.values().stream().sorted(MOST_RECENT_FIRST).toList();
    }

    public List<String> hashes() {
        return byHash.values().stream().sorted(MOST_RECENT_FIRST).limit(LIMIT).map(DeviceRow::deviceHash).toList();
    }

    public Optional<Instant> lastEventAt() {
        return byHash.values().stream().map(DeviceRow::eventAt).max(Comparator.naturalOrder());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof KnownDevices that && byHash.equals(that.byHash);
    }

    @Override
    public int hashCode() {
        return byHash.hashCode();
    }
}
