package br.com.motorfraude.customer.adapters.controller.cdc;

import static br.com.motorfraude.customer.adapters.controller.cdc.DebeziumCustomerMapperTest.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DebeziumDeviceMapperTest {

    @Test
    @DisplayName("criação (c): vira o dispositivo com o first_seen_at e o instante do evento")
    void criacao() {
        var change = DebeziumDeviceMapper.parse(fixture("device-c.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("c");
        assertThat(change.row().accountId()).isEqualTo("acc_tok_fixt0001");
        assertThat(change.row().deviceHash()).isEqualTo("dev_fixt00000001");
        assertThat(change.row().firstSeenAt()).isEqualTo(Instant.parse("2026-10-02T10:00:00Z"));
        assertThat(change.row().eventAt()).isEqualTo(Instant.ofEpochMilli(1794698361212L));
    }

    @Test
    @DisplayName("leitura do snapshot (r): é tratada como um upsert")
    void snapshot() {
        var change = DebeziumDeviceMapper.parse(fixture("device-r.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("r");
        assertThat(change.row().deviceHash()).startsWith("dev_");
    }

    @Test
    @DisplayName("remoção (d): sem linha, porque o before só tem a chave e first_seen_at de 1970")
    void remocao() {
        var change = DebeziumDeviceMapper.parse(fixture("device-d.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("d");
        assertThat(change.row()).isNull();
    }

    @Test
    @DisplayName("o tombstone do Debezium (valor nulo) não gera mudança")
    void tombstone() {
        assertThat(DebeziumDeviceMapper.parse(null)).isEmpty();
    }

    @Test
    @DisplayName("a chave composta do Debezium vira a chave da linha: conta e dispositivo")
    void chave() {
        var key = DebeziumDeviceMapper.rowKey(fixture("device-key.json"));

        assertThat(key).isEqualTo("acc_tok_fixt0001|dev_fixt00000001");
        assertThat(DebeziumDeviceMapper.accountIdOf(key)).isEqualTo("acc_tok_fixt0001");
    }

    @Test
    @DisplayName("JSON inválido ou sem campo obrigatório é rejeitado")
    void invalido() {
        assertThatThrownBy(() -> DebeziumDeviceMapper.parse("{não é json")).isInstanceOf(CdcFormatException.class);
        assertThatThrownBy(() -> DebeziumDeviceMapper.parse("{\"op\":\"c\",\"ts_ms\":1,\"after\":{\"account_token\":\"a\"}}"))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("device_hash");
        assertThatThrownBy(() -> DebeziumDeviceMapper.rowKey("{\"account_token\":\"a\"}")).isInstanceOf(CdcFormatException.class);
    }
}
