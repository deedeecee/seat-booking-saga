CREATE TABLE service_info (
    info_key   TEXT PRIMARY KEY,
    info_value TEXT NOT NULL
);

INSERT INTO service_info (info_key, info_value) VALUES ('service', 'payment-service');