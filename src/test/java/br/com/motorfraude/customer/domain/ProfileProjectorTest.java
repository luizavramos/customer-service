package br.com.motorfraude.customer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.Segment;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ProfileProjectorTest {

    private static final Instant OPENED = Instant.parse("2026-03-07T21:22:46Z");
    private static final Instant CHANGED = Instant.parse("2026-09-08T21:22:46Z");
    private static final Instant CUSTOMER_EVENT = Instant.parse("2026-11-01T10:00:00Z");
    private static final Instant DEVICE_EVENT = Instant.parse("2026-11-02T10:00:00Z");

    private static CustomerRow customer(Instant changedAt, Instant eventAt) {
        return new CustomerRow("acc_tok_0001", Segment.PF, OPENED, changedAt, eventAt);
    }

    private static KnownDevices devices(String... hashes) {
        var result = KnownDevices.empty();
        int day = 0;
        for (var hash : hashes) {
            var seen = OPENED.plusSeconds(++day * 86_400L);
            result = result.add(new DeviceRow("acc_tok_0001", hash, seen, DEVICE_EVENT));
        }
        return result;
    }

    @Test
    @DisplayName("copia os campos do cliente e lista os dispositivos, do mais recente ao mais antigo")
    void campos() {
        var profile = ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), devices("dev_aaaaaaaa", "dev_bbbbbbbb"));

        assertThat(profile.accountId()).isEqualTo("acc_tok_0001");
        assertThat(profile.segment()).isEqualTo(Segment.PF);
        assertThat(profile.accountOpenedAt()).isEqualTo(OPENED);
        assertThat(profile.lastCredentialChangeAt()).isEqualTo(CHANGED);
        assertThat(profile.knownDeviceHashes()).containsExactly("dev_bbbbbbbb", "dev_aaaaaaaa");
    }

    @Test
    @DisplayName("troca de credencial nula continua nula")
    void credencialNula() {
        var profile = ProfileProjector.project(customer(null, CUSTOMER_EVENT), devices("dev_aaaaaaaa"));

        assertThat(profile.lastCredentialChangeAt()).isNull();
    }

    @Test
    @DisplayName("cliente sem nenhum dispositivo gera perfil com lista vazia")
    void semDispositivos() {
        assertThat(ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), KnownDevices.empty()).knownDeviceHashes()).isEmpty();
        assertThat(ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), null).knownDeviceHashes()).isEmpty();
    }

    @Test
    @DisplayName("updatedAt é o evento mais recente entre o cliente e os dispositivos, não o relógio")
    void updatedAtMaiorEvento() {
        assertThat(ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), devices("dev_aaaaaaaa")).updatedAt()).isEqualTo(DEVICE_EVENT);
        assertThat(ProfileProjector.project(customer(CHANGED, DEVICE_EVENT.plusSeconds(60)), devices("dev_aaaaaaaa")).updatedAt())
                .isEqualTo(DEVICE_EVENT.plusSeconds(60));
        assertThat(ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), null).updatedAt()).isEqualTo(CUSTOMER_EVENT);
    }

    @Test
    @DisplayName("é determinística: a mesma entrada gera o mesmo perfil")
    void deterministica() {
        var a = ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), devices("dev_aaaaaaaa", "dev_bbbbbbbb"));
        var b = ProfileProjector.project(customer(CHANGED, CUSTOMER_EVENT), devices("dev_aaaaaaaa", "dev_bbbbbbbb"));

        assertThat(a).isEqualTo(b);
    }

    @Test
    @DisplayName("sem cliente não há perfil")
    void semCliente() {
        assertThatThrownBy(() -> ProfileProjector.project(null, KnownDevices.empty())).isInstanceOf(IllegalArgumentException.class);
    }
}
