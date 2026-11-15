CREATE SCHEMA bank;

CREATE TABLE bank.customers (
    account_token             text PRIMARY KEY,
    customer_token            text        NOT NULL,
    full_name                 text        NOT NULL,
    cpf                       text        NOT NULL,
    email                     text        NOT NULL,
    phone                     text        NOT NULL,
    segment                   text        NOT NULL CHECK (segment IN ('PF', 'PJ')),
    account_opened_at         timestamptz NOT NULL,
    last_credential_change_at timestamptz
);

CREATE TABLE bank.customer_devices (
    account_token text        NOT NULL REFERENCES bank.customers (account_token),
    device_hash   text        NOT NULL,
    first_seen_at timestamptz NOT NULL,
    PRIMARY KEY (account_token, device_hash)
);

CREATE PUBLICATION dbz_customers FOR TABLE bank.customers, bank.customer_devices;
