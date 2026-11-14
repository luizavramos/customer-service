package br.com.motorfraude.customer.adapters.controller.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.Segment;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DebeziumCustomerMapperTest {

    static String fixture(String name) {
        try (var in = DebeziumCustomerMapperTest.class.getResourceAsStream("/debezium/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("criação (c): vira o cliente com só os campos permitidos")
    void criacao() {
        var change = DebeziumCustomerMapper.parse(fixture("customer-c.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("c");
        assertThat(change.row().accountId()).isEqualTo("acc_tok_fixt0001");
        assertThat(change.row().segment()).isEqualTo(Segment.PF);
        assertThat(change.row().accountOpenedAt()).isEqualTo(Instant.parse("2026-10-01T10:00:00Z"));
        assertThat(change.row().lastCredentialChangeAt()).isNull();
        assertThat(change.row().eventAt()).isEqualTo(Instant.ofEpochMilli(1794698361173L));
    }

    @Test
    @DisplayName("atualização (u): traz o novo segmento e a troca de credencial")
    void atualizacao() {
        var change = DebeziumCustomerMapper.parse(fixture("customer-u.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("u");
        assertThat(change.row().segment()).isEqualTo(Segment.PJ);
        assertThat(change.row().lastCredentialChangeAt()).isEqualTo(Instant.parse("2026-10-05T12:00:00Z"));
        assertThat(change.row().eventAt()).isEqualTo(Instant.ofEpochMilli(1794698361214L));
    }

    @Test
    @DisplayName("leitura do snapshot (r): é tratada como um upsert")
    void snapshot() {
        var change = DebeziumCustomerMapper.parse(fixture("customer-r.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("r");
        assertThat(change.row().accountId()).startsWith("acc_tok_");
        assertThat(change.row().segment()).isIn(Segment.PF, Segment.PJ);
    }

    @Test
    @DisplayName("remoção (d): não há linha; o before só tem a chave e valores vazios que não podem virar dado")
    void remocao() {
        var change = DebeziumCustomerMapper.parse(fixture("customer-d.json")).orElseThrow();

        assertThat(change.op()).isEqualTo("d");
        assertThat(change.row()).isNull();
    }

    @Test
    @DisplayName("o tombstone do Debezium (valor nulo) não gera mudança")
    void tombstone() {
        assertThat(DebeziumCustomerMapper.parse(null)).isEmpty();
    }

    @Test
    @DisplayName("a chave do Debezium dá o accountId")
    void chave() {
        assertThat(DebeziumCustomerMapper.accountId(fixture("customer-key.json"))).isEqualTo("acc_tok_fixt0001");
    }

    @Test
    @DisplayName("nome, cpf, e-mail e telefone nunca aparecem no resultado (LGPD)")
    void descartaDadoPessoal() {
        for (var name : new String[] {"customer-c.json", "customer-u.json", "customer-r.json"}) {
            var change = DebeziumCustomerMapper.parse(fixture(name)).orElseThrow();
            var texto = change.toString();

            assertThat(texto).doesNotContain("Fulano", "Cliente Sintetico", "111.222.333-96", "exemplo.invalid", "+55119")
                    .doesNotContainPattern("\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}");
            assertThat(change.row().getClass().getRecordComponents()).extracting(c -> c.getName())
                    .containsExactly("accountId", "segment", "accountOpenedAt", "lastCredentialChangeAt", "eventAt");
        }
    }

    @Test
    @DisplayName("um campo novo no CDC é ignorado, não quebra nem vaza")
    void campoNovo() {
        var json = fixture("customer-c.json").replace("\"full_name\"", "\"campo_novo\":\"x\",\"full_name\"");

        assertThat(DebeziumCustomerMapper.parse(json).orElseThrow().row().accountId()).isEqualTo("acc_tok_fixt0001");
    }

    @Test
    @DisplayName("JSON inválido ou sem campo obrigatório é rejeitado com mensagem clara")
    void invalido() {
        assertThatThrownBy(() -> DebeziumCustomerMapper.parse("{não é json")).isInstanceOf(CdcFormatException.class);
        assertThatThrownBy(() -> DebeziumCustomerMapper.parse("{\"op\":\"c\",\"ts_ms\":1,\"after\":{\"account_token\":\"a\"}}"))
                .isInstanceOf(CdcFormatException.class).hasMessageContaining("segment");
        assertThatThrownBy(() -> DebeziumCustomerMapper.parse("{\"op\":\"x\",\"ts_ms\":1}")).isInstanceOf(CdcFormatException.class)
                .hasMessageContaining("op");
    }
}
