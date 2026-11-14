package br.com.motorfraude.customer.adapters.gateway.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.customer.domain.CustomerRow;
import br.com.motorfraude.customer.domain.DeviceRow;
import br.com.motorfraude.customer.domain.KnownDevices;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StateSerdesTest {

    private static final Instant T = Instant.parse("2026-04-01T10:00:00.123456Z");

    @Test
    @DisplayName("o cliente sobrevive à ida e volta pelo estado, com instantes e campo nulo")
    void cliente() {
        var row = new CustomerRow("acc_tok_0001", Segment.PJ, T, null, T.plusSeconds(1));
        var serde = StateSerdes.customer();

        assertThat(serde.deserializer().deserialize("t", serde.serializer().serialize("t", row))).isEqualTo(row);
    }

    @Test
    @DisplayName("o conjunto de dispositivos sobrevive à ida e volta, inclusive vazio e nulo")
    void dispositivos() {
        var known = KnownDevices.of(java.util.List.of(
                new DeviceRow("acc_tok_0001", "dev_aaaaaaaa", T, T), new DeviceRow("acc_tok_0001", "dev_bbbbbbbb", T.plusSeconds(5), T)));
        var serde = StateSerdes.knownDevices();

        assertThat(serde.deserializer().deserialize("t", serde.serializer().serialize("t", known))).isEqualTo(known);
        assertThat(serde.deserializer().deserialize("t", serde.serializer().serialize("t", KnownDevices.empty()))).isEqualTo(KnownDevices.empty());
        assertThat(serde.serializer().serialize("t", null)).isNull();
        assertThat(serde.deserializer().deserialize("t", null)).isNull();
    }
}
