CREATE TABLE IF NOT EXISTS deaise.bopo_stg_terminals (
	terminal_id varchar(100),
	terminal_type varchar(100),
	terminal_city varchar(100),
	terminal_address varchar(100),
	update_dt DATE
);



CREATE TABLE IF NOT EXISTS deaise.bopo_stg_blacklist (
	passport_num varchar(100),
	entry_dt DATE
);



CREATE TABLE IF NOT EXISTS deaise.bopo_stg_transactions (
	trans_id varchar(100),
	trans_date TIMESTAMP,
	card_num varchar(100),
	oper_type varchar(100),
	amt DECIMAL,
	oper_result varchar(100),
	terminal varchar(100),
	update_dt DATE
);



CREATE TABLE IF NOT EXISTS deaise.bopo_stg_cards (
	card_num varchar(100),
	account_num varchar(100),
	create_dt DATE,
	update_dt DATE
);



CREATE TABLE IF NOT EXISTS deaise.bopo_stg_accounts (
	account_num varchar(100),
	valid_to DATE,
	client varchar(100),
	create_dt DATE,
	update_dt DATE
);



CREATE TABLE IF NOT EXISTS deaise.bopo_stg_clients (
	client_id varchar(100),
	last_name varchar(100),
	first_name varchar(100),
	patronimic varchar(100),
	date_of_birth DATE,
	passport_num varchar(100),
	passport_valid_to DATE,
	phone varchar(100),
	create_dt DATE,
	update_dt DATE
);

CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_fact_transactions (
	trans_id varchar(100),
	trans_date TIMESTAMP,
	card_num varchar(100),
	oper_type varchar(100),
	amt DECIMAL,
	oper_result varchar(100),
	terminal varchar(100)
);
CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_fact_passport_blacklist (
	passport_num varchar(100),
	entry_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_dim_terminals (
	terminal_id varchar(100),
	terminal_type varchar(100),
	terminal_city varchar(100),
	terminal_address varchar(100),
	create_dt DATE,
	update_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_dim_cards (
	card_num varchar(100),
	account_num varchar(100),
	create_dt DATE,
	update_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_dim_accounts (
	account_num varchar(100),
	valid_to DATE,
	client varchar(100),
	create_dt DATE,
	update_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_dwh_dim_clients (
	client_id varchar(100),
	last_name varchar(100),
	first_name varchar(100),
	patronimic varchar(100),
	date_of_birth DATE,
	passport_num varchar(100),
	passport_valid_to DATE,
	phone varchar(100),
	create_dt DATE,
	update_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_rep_fraud (
	event_dt TIMESTAMP,
	passport varchar(100),
	fio varchar(100),
	phone varchar(100),
	event_type varchar(4),
	report_dt DATE
);
CREATE TABLE IF NOT EXISTS deaise.bopo_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);
INSERT INTO deaise.bopo_meta( schema_name, table_name, max_update_dt )
VALUES	( 'deaise','bopo_clients', to_timestamp('1900-01-01','YYYY-MM-DD') ),
		( 'deaise','bopo_accounts', to_timestamp('1900-01-01','YYYY-MM-DD') ),
		( 'deaise','bopo_cards', to_timestamp('1900-01-01','YYYY-MM-DD') ),
		( 'deaise','bopo_terminals', to_timestamp('1900-01-01','YYYY-MM-DD') );

CREATE TABLE IF NOT EXISTS deaise.bopo_stg_cards_del (
	card_num varchar
);

CREATE TABLE IF NOT EXISTS deaise.bopo_stg_accounts_del (
	account varchar
);

CREATE TABLE IF NOT EXISTS deaise.bopo_stg_clients_del (
	client_id varchar
);









