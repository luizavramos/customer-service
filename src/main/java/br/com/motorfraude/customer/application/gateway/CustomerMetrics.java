package br.com.motorfraude.customer.application.gateway;

import java.time.Instant;

public interface CustomerMetrics {

    CustomerMetrics NOOP = new CustomerMetrics() {
        @Override
        public void cdcEvent(String table, String op) {
        }

        @Override
        public void profilePublished(Instant eventAt) {
        }

        @Override
        public void tombstonePublished() {
        }
    };

    void cdcEvent(String table, String op);

    void profilePublished(Instant eventAt);

    void tombstonePublished();
}
