# customer-service

Projeta o cadastro do banco legado (CDC do Debezium em `cdc.bank.customers` e `cdc.bank.customer_devices`) no perfil do cliente publicado em `customer-profile.v1` (tópico compactado, chave = `customerId`). Kafka Streams com exactly-once, sem Spring; saúde e métricas em `/actuator/...` pela porta 8083.

Build: `./mvnw verify`. Camadas: `domain` ← `application` ← `adapters` ← `infrastructure` (ArchUnit).
