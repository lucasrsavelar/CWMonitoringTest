CREATE TABLE sales (
	sale_id INTEGER,
	origin INTEGER,
	sale_time VARCHAR(3),
	today INTEGER,
	yesterday INTEGER,
	same_day_last_week INTEGER,
	avg_last_week FLOAT,
	avg_last_month FLOAT,
	PRIMARY KEY (sale_id)
);

CREATE SEQUENCE sale_id_seq AS INTEGER START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 999999;

ALTER TABLE sales
ALTER COLUMN sale_id
SET DEFAULT nextval('sale_id_seq');

ALTER SEQUENCE sale_id_seq
OWNED BY sales.sale_id;

SELECT * FROM sales;
