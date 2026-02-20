-------------------------- CRIAÇÃO DA TABELA DE TRANSAÇÕES NO BANCO --------------------------

CREATE TABLE transactions (
	transaction_id INT,
	date_hour text,
	status VARCHAR(50),
	amount text,
	PRIMARY KEY (transaction_id)
);

CREATE SEQUENCE transaction_id_seq AS INTEGER START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 999999;

ALTER TABLE transactions
ALTER COLUMN transaction_id
SET DEFAULT nextval('transaction_id_seq');

ALTER SEQUENCE transaction_id_seq
OWNED BY transactions.transaction_id;

DELETE FROM transactions WHERE transaction_id = 1;

ALTER TABLE transactions
ALTER COLUMN date_hour
TYPE TIMESTAMP
USING date_hour::timestamp;

ALTER TABLE transactions
ALTER COLUMN amount
TYPE INTEGER
USING amount::INTEGER;

-------------------------- CRIAÇÃO DA TABELA DE ANOMALIAS NO BANCO --------------------------

CREATE TABLE anomalies (
	anomaly_id INTEGER,
	date_hour timestamp,
	main_feature VARCHAR(50),
	anomaly_message text,
	ensemble_score float,
	PRIMARY KEY (anomaly_id)
);

CREATE SEQUENCE anomaly_id_seq AS INTEGER START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 999999;

ALTER SEQUENCE anomaly_id_seq
OWNED BY anomalies.anomaly_id;

-------------------------- MANIPULAÇÃO DE DADOS --------------------------

SELECT * FROM TRANSACTIONS;

SELECT DISTINCT status FROM transactions;

SELECT
	date_hour,
	SUM(CASE WHEN status = 'approved' THEN amount ELSE 0 END) AS approved,
    SUM(CASE WHEN status = 'denied' THEN amount ELSE 0 END) AS denied,
	SUM(CASE WHEN status = 'failed' THEN amount ELSE 0 END) AS failed,
	SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END) AS refunded,
	SUM(CASE WHEN status = 'reversed' THEN amount ELSE 0 END) AS reversed,
	SUM(CASE WHEN status = 'backend_reversed' THEN amount ELSE 0 END) AS backend_reversed,
	SUM(amount) AS total_transactions
FROM transactions
GROUP BY date_hour
ORDER BY date_hour desc;

SELECT
	status,
	MAX(amount)
FROM transactions
GROUP BY status;

WITH aggregated AS (
    SELECT
        date_hour,
        SUM(CASE WHEN status = 'denied' THEN amount ELSE 0 END) AS denied,
        SUM(CASE WHEN status = 'failed' THEN amount ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'refunded' THEN amount ELSE 0 END) AS refunded,
        SUM(CASE WHEN status = 'reversed' THEN amount ELSE 0 END) AS reversed,
        SUM(CASE WHEN status = 'backend_reversed' THEN amount ELSE 0 END) AS backend_reversed,
        SUM(amount) AS total
    FROM transactions
    GROUP BY date_hour
)

SELECT
    MAX(denied::float / NULLIF(total, 0)) AS max_denied_rate,
    MAX(failed::float / NULLIF(total, 0)) AS max_failed_rate,
    MAX(refunded::float / NULLIF(total, 0)) AS max_refunded_rate,
    MAX(reversed::float / NULLIF(total, 0)) AS max_reversed_rate,
    MAX(backend_reversed::float / NULLIF(total, 0)) AS max_backend_reversed_rate
FROM aggregated;