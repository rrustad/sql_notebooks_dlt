CREATE DATABASE if not exists riley_sql_notebooks;
-- create bronze layer
-- trunc and replace raw transactions
CREATE OR REPLACE TABLE riley_sql_notebooks.raw_txs as SELECT * FROM `json`.`/demos/dlt/loans/raw_transactions`;

-- trunc and replace raw transactions
CREATE OR REPLACE TABLE riley_sql_notebooks.ref_accounting_treatment as SELECT * FROM `delta`.`/demos/dlt/loans/ref_accounting_treatment`;

CREATE TEMP VIEW raw_historical_loans_v
USING CSV
OPTIONS (path "/demos/dlt/loans/historical_loans", header "true", mode "FAILFAST");

-- trunc and replace raw transactions
CREATE OR REPLACE TABLE  riley_sql_notebooks.raw_historical_loans as SELECT * FROM raw_historical_loans_v;

-- create silver layer
CREATE OR REPLACE VIEW riley_sql_notebooks.new_txs 
AS SELECT txs.*, ref.accounting_treatment as accounting_treatment FROM riley_sql_notebooks.raw_txs txs
  INNER JOIN riley_sql_notebooks.ref_accounting_treatment ref ON txs.accounting_treatment_id = ref.id;

CREATE OR REPLACE TABLE riley_sql_notebooks.cleaned_new_txs 
AS SELECT * from riley_sql_notebooks.new_txs
where 
(next_payment_date > date('2020-12-31')) OR
(balance > 0 AND arrears_balance > 0) OR
cost_center_code IS NOT NULL;

CREATE OR REPLACE TABLE riley_sql_notebooks.quarantine_bad_txs 
AS SELECT * from riley_sql_notebooks.new_txs
where 
(next_payment_date <= date('2020-12-31')) OR
(balance <= 0 OR arrears_balance <= 0) OR
cost_center_code IS NULL;

CREATE OR REPLACE VIEW riley_sql_notebooks.historical_txs 
AS SELECT l.*, ref.accounting_treatment as accounting_treatment FROM riley_sql_notebooks.raw_historical_loans l INNER JOIN riley_sql_notebooks.ref_accounting_treatment ref ON l.accounting_treatment_id = ref.id;

-- create gold layer
CREATE TABLE riley_sql_notebooks.total_loan_balances
AS SELECT sum(revol_bal)  AS bal, addr_state   AS location_code FROM riley_sql_notebooks.historical_txs  GROUP BY addr_state
  UNION SELECT sum(balance) AS bal, country_code AS location_code FROM riley_sql_notebooks.cleaned_new_txs GROUP BY country_code;

CREATE VIEW riley_sql_notebooks.new_loan_balances_by_cost_center
AS SELECT sum(balance) sum_balance, cost_center_code FROM riley_sql_notebooks.cleaned_new_txs
  GROUP BY cost_center_code;

CREATE VIEW new_loan_balances_by_country
AS SELECT sum(count) sum_count, country_code FROM riley_sql_notebooks.cleaned_new_txs GROUP BY country_code;