---------------------------------------------------------------------------------
	-- скрипты для создания структур
---------------------------------------------------------------------------------

create table deaian.ptrv_stg_accounts( 
	account_num varchar(20),
	valid_to date,
	client varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_stg_cards( 
	card_num varchar(19),
	account_num varchar(20),
	create_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_stg_clients( 
	client_id varchar(10),
	lastname varchar(20),
	firstname varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	create_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_stg_terminals( 
	terminal_id varchar(6),
	terminal_type varchar(3),
	terminal_city varchar(25),
	terminal_address varchar(60),
	create_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_stg_passport_blacklist( 
	date date,
	passport_num varchar(15),
	create_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_stg_transactions (
	trans_id varchar(60),
	card_num varchar(19),
	trans_date timestamp(0),
	oper_type varchar(11),
	amt decimal(18,2),
	oper_result varchar(10),
	terminal varchar(6),
	create_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_fact_passport_blacklist (
	passport_num varchar(15),
	entry_dt date,
	create_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_fact_transactions (
	trans_id varchar(60),
	card_num varchar(19),
	trans_date timestamp(0),
	oper_type varchar(11),
	amt decimal(18,2),
	oper_result varchar(10),
	terminal varchar(6),
	create_dt timestamp(0),
	update_dt timestamp(0),
	processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_dim_accounts_hist (
	account_num varchar(20),
	valid_to date, 
	client varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1),
    processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_dim_cards_hist (
	card_num varchar(19),
	account_num varchar(20),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1),
	processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_dim_clients_hist (
	client_id varchar(10),
	lastname varchar(20),
	firstname varchar(20),
	patronymic varchar(20),
	date_of_birth date,
	passport_num varchar(15),
	passport_valid_to date,
	phone varchar(16),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1),
	processed_dt timestamp(0)
);

create table deaian.ptrv_dwh_dim_terminals_hist (
	terminal_id varchar(6),
	terminal_type varchar(3),
	terminal_city varchar(25),
	terminal_address varchar(60),
	effective_from timestamp(0),
	effective_to timestamp(0),
	deleted_flg varchar(1),
	processed_dt timestamp(0)
);

create table deaian.ptrv_rep_fraud (
	event_dt timestamp(0),
	passport varchar(15),
	fio varchar(60),
	phone varchar(16),
	event_type varchar(1),
	report_dt date
);

create table deaian.ptrv_meta_transactions (
	schema_name varchar(6),
	table_name varchar(50),
	update_dt timestamp(0)
);