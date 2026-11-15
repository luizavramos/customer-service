# customer-service

Projeta o cadastro do banco legado no **perfil do cliente**, publicado em `customer-profile.v1` (tópico compactado, chave = `accountId`). O motor de detecção (spec 004) lê esse tópico como tabela. Kafka Streams com exactly-once, sem Spring.

```
cdc.bank.customers ──────────────► KTable de clientes ─────────────┐
cdc.bank.customer_devices ─► KTable por dispositivo ─► agrega por conta ─► left join ─► customer-profile.v1
   (Debezium, JSON sem schema)        (somador/subtrator: 10 mais recentes)
```

- **Sem dado pessoal (LGPD):** os mapeadores leem só `account_token`, `segment`, `account_opened_at` e `last_credential_change_at` (clientes) e `account_token`, `device_hash`, `first_seen_at` (dispositivos). Nome, CPF, e-mail e telefone existem no CDC de propósito e nunca entram no estado do Kafka Streams nem no perfil.
- **Perfil:** `knownDeviceHashes` traz os 10 dispositivos mais recentes por `first_seen_at`; `updatedAt` é o maior `ts_ms` do CDC entre o cliente e os dispositivos (sem relógio do serviço, então o resultado é reproduzível).
- **Remoção:** apagar o cliente publica um *tombstone* (valor nulo) na mesma chave. Apagar um dispositivo o tira da lista e traz de volta o 11º.
- **Evento malformado:** é descartado, contado em `customer_cdc_events_total{op="invalid"}` e registrado no log (sem o conteúdo); não derruba o serviço.

## Configuração

| Variável | Padrão | Para quê |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | brokers |
| `HTTP_PORT` | `8080` (no compose: `8083`) | saúde e métricas |
| `STATE_DIR` | `/data` | RocksDB do Kafka Streams (volume persistente) |
| `REPLICATION_FACTOR` | `1` | tópicos internos (3 em produção) |
| `STANDBY_REPLICAS` | `0` | réplicas de estado (1 ou mais em produção) |
| `HOSTNAME` | `customer-service-0` | identifica a instância (static membership) |

Saúde: `/actuator/health/liveness`, `/actuator/health/readiness`. Métricas: `/actuator/prometheus`.

## Rodar

No repositório `motor-fraude` (compose), com o ambiente de pé:

```bash
./mvnw -DskipTests package
sh infra/connect/register-connectors.sh          # registra o conector Debezium dos clientes
docker compose --profile app up -d --build customer-service
```

O jar executável é `target/customer-service.jar`. A imagem usa a Temurin padrão (e não a Alpine) porque o RocksDB precisa de glibc.

## Métricas

| Métrica | Significado |
|---|---|
| `customer_cdc_events_total{table,op}` | eventos do CDC lidos (`r` snapshot, `c`, `u`, `d`; `invalid` = descartado) |
| `customer_profiles_published_total{kind}` | `profile` = perfil publicado, `tombstone` = remoção |
| `customer_cdc_lag_seconds` | atraso entre o `ts_ms` do evento e a publicação (histograma; meta SC-001: p99 ≤ 5 s) |

O painel **Perfil do cliente (spec 003)** do Grafana mostra as três. Durante o snapshot inicial ou uma reconstrução o atraso sobe, porque os eventos relidos são antigos: não é falha.

Cada evento de dispositivo republica o perfil da conta, então `customer_profiles_published_total` passa do número de clientes; a compactação do tópico guarda só a última versão de cada chave.

## Reconstruir os perfis do zero (H4)

Use quando a lógica de projeção tinha um erro e os perfis publicados ficaram errados. Depois de corrigir e implantar o serviço:

```bash
sh infra/connect/rebuild-customers-cdc.sh
```

O script (no repositório `motor-fraude`) para o conector `customers-cdc`, apaga os offsets dele no Connect e o slot de replicação `customers_cdc_slot` do Postgres, e retoma o conector. Sem offsets, o Debezium faz um snapshot inicial das duas tabelas e o serviço republica todos os perfis nas mesmas chaves. Conferido no compose com 10 mil clientes e 25 mil dispositivos: o contador de snapshot (`customer_cdc_events_total{op="r"}`) subiu de novo em 10 mil clientes e os perfis foram republicados.

- O conector **não é removido** de propósito: recriar um conector logo depois de removê-lo parado faz o Connect responder 500 (NullPointerException) por alguns minutos.
- Enquanto o snapshot roda, o motor continua lendo os perfis antigos, que são sobrescritos aos poucos; não há janela sem perfil.
- Se o erro estava no **estado** do Kafka Streams (e não só na lógica), pare o serviço, apague o volume `customer-service-data` e redefina o grupo de consumo (`kafka-streams-application-reset.sh --application-id customer-service`) antes de rodar o script.
- Um cliente removido do banco antes da reconstrução já tem *tombstone* no tópico; o snapshot não o traz de volta.

## Testes

```bash
./mvnw verify
```

Domínio, mapeadores com fixtures reais do Debezium, topologia (`TopologyTestDriver`), privacidade (10 mil clientes sintéticos), métricas, ArchUnit e um teste de integração com Postgres (`wal_level=logical`), Kafka e Debezium Connect reais (Testcontainers) que mede o `UPDATE` até o perfil publicado.
