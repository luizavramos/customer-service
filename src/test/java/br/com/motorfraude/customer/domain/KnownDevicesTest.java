package br.com.motorfraude.customer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KnownDevicesTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private static DeviceRow device(String hash, int daysAfterT0) {
        var seen = T0.plusSeconds(daysAfterT0 * 86_400L);
        return new DeviceRow("acc_tok_0001", hash, seen, seen.plusSeconds(5));
    }

    @Test
    @DisplayName("começa vazio, sem instante de evento")
    void vazio() {
        var devices = KnownDevices.empty();

        assertThat(devices.hashes()).isEmpty();
        assertThat(devices.lastEventAt()).isEmpty();
    }

    @Test
    @DisplayName("lista os dispositivos do mais recente para o mais antigo por first_seen_at")
    void ordem() {
        var devices = KnownDevices.empty()
                .add(device("dev_aaaaaaaa", 3))
                .add(device("dev_bbbbbbbb", 9))
                .add(device("dev_cccccccc", 1));

        assertThat(devices.hashes()).containsExactly("dev_bbbbbbbb", "dev_aaaaaaaa", "dev_cccccccc");
    }

    @Test
    @DisplayName("devolve só os 10 mais recentes quando a conta tem 15")
    void limiteDeDez() {
        var devices = KnownDevices.empty();
        for (int i = 1; i <= 15; i++) {
            devices = devices.add(device("dev_%08d".formatted(i), i));
        }

        assertThat(devices.hashes()).hasSize(10);
        assertThat(devices.hashes().getFirst()).isEqualTo("dev_00000015");
        assertThat(devices.hashes().getLast()).isEqualTo("dev_00000006");
    }

    @Test
    @DisplayName("o resultado não depende da ordem de chegada dos eventos")
    void independeDaOrdemDeChegada() {
        var a = KnownDevices.empty().add(device("dev_aaaaaaaa", 1)).add(device("dev_bbbbbbbb", 2)).add(device("dev_cccccccc", 3));
        var b = KnownDevices.empty().add(device("dev_cccccccc", 3)).add(device("dev_aaaaaaaa", 1)).add(device("dev_bbbbbbbb", 2));

        assertThat(a).isEqualTo(b);
    }

    @Test
    @DisplayName("o mesmo dispositivo duas vezes conta uma vez")
    void mesmoDispositivo() {
        var devices = KnownDevices.empty().add(device("dev_aaaaaaaa", 1)).add(device("dev_aaaaaaaa", 1));

        assertThat(devices.hashes()).containsExactly("dev_aaaaaaaa");
    }

    @Test
    @DisplayName("empate em first_seen_at desempata pelo hash, para o resultado ser reproduzível")
    void empate() {
        var devices = KnownDevices.empty().add(device("dev_bbbbbbbb", 2)).add(device("dev_aaaaaaaa", 2));

        assertThat(devices.hashes()).containsExactly("dev_aaaaaaaa", "dev_bbbbbbbb");
    }

    @Test
    @DisplayName("remover um dispositivo o tira da lista e traz de volta o 11º que estava de fora")
    void remocao() {
        var devices = KnownDevices.empty();
        for (int i = 1; i <= 11; i++) {
            devices = devices.add(device("dev_%08d".formatted(i), i));
        }

        var depois = devices.remove(device("dev_00000011", 11));

        assertThat(depois.hashes()).doesNotContain("dev_00000011").hasSize(10).contains("dev_00000001");
    }

    @Test
    @DisplayName("remover um dispositivo desconhecido não muda a lista")
    void removerDesconhecido() {
        var devices = KnownDevices.empty().add(device("dev_aaaaaaaa", 1));

        assertThat(devices.remove(device("dev_zzzzzzzz", 2)).hashes()).containsExactly("dev_aaaaaaaa");
    }

    @Test
    @DisplayName("guarda o instante do evento mais recente, inclusive o de uma remoção")
    void instanteDoUltimoEvento() {
        var devices = KnownDevices.empty().add(device("dev_aaaaaaaa", 1)).add(device("dev_bbbbbbbb", 5));
        assertThat(devices.lastEventAt()).contains(T0.plusSeconds(5 * 86_400L + 5));

        var removido = devices.remove(new DeviceRow("acc_tok_0001", "dev_aaaaaaaa", T0, T0.plusSeconds(10 * 86_400L)));
        assertThat(removido.lastEventAt()).contains(T0.plusSeconds(10 * 86_400L));
    }

    @Test
    @DisplayName("um evento atrasado não faz o instante do evento voltar no tempo")
    void instanteNaoRecua() {
        var devices = KnownDevices.empty().add(device("dev_bbbbbbbb", 5)).add(device("dev_aaaaaaaa", 1));

        assertThat(devices.lastEventAt()).contains(T0.plusSeconds(5 * 86_400L + 5));
    }

    @Test
    @DisplayName("a lista devolvida não pode ser alterada por fora")
    void imutavel() {
        var devices = KnownDevices.empty().add(device("dev_aaaaaaaa", 1));

        assertThatThrownBy(() -> devices.hashes().add("dev_x")).isInstanceOf(UnsupportedOperationException.class);
    }
}
