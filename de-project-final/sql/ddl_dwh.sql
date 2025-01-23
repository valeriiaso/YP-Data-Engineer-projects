--global_metrics
create table STV2023121116__DWH.global_metrics(
	id								identity primary key,
	date_update						date not null,
	currency_from					int not null,
	amount_total					numeric(17, 2) not null,
	cnt_transactions				int not null check(cnt_transactions >= 0),
	avg_transactions_per_account	numeric(15, 2) not null check(avg_transactions_per_account >= 0),
	cnt_accounts_make_transactions	int not null check(cnt_accounts_make_transactions >= 0),
	unique(date_update, currency_from)
)
order by date_update
segmented by hash(date_update, currency_from) all nodes ksafe 1
partition by date_update
group by calendar_hierarchy_day(date_update, 3, 2);


--h_transactions
create table STV2023121116__DWH.h_transactions(
	h_transaction_pk	bigint primary key,
	operation_id		varchar(60) not null,
	load_dt				timestamp not null,
	load_src			varchar(20) not null
)
order by load_dt
segmented by h_transaction_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.h_currencies(
	h_currency_pk	bigint primary key,
	currency_code	int not null,
	load_dt			timestamp not null,
	load_src		varchar not null
)
order by load_dt
segmented by h_currency_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.l_transaction_currency(
	hk_transaction_currency_pk	bigint primary key,
	h_transaction_pk			bigint not null references STV2023121116__DWH.h_transactions(h_transaction_pk),
	h_currency_pk				bigint not null references STV2023121116__DWH.h_currencies(h_currency_pk),
	load_dt						timestamp not null,
	load_src					varchar not null
)
order by load_dt
segmented by hk_transaction_currency_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.s_currencies_rates(
	h_currency_pk					bigint references STV2023121116__DWH.h_currencies(h_currency_pk),
	date_update						timestamp(3) not null,
	currency_code_with				int not null,
	currency_with_div				numeric(5, 3) not null,
	load_dt							timestamp not null,
	load_src						varchar not null,
	primary key (h_currency_pk, load_dt)
)
order by load_dt
segmented by h_currency_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.s_transactions_send_info(
	h_transaction_pk					bigint references STV2023121116__DWH.h_transactions(h_transaction_pk),
	account_number_from					int not null,
	account_number_to					int not null,
	country								varchar(30),
	transaction_type					varchar(30),
	load_dt								timestamp not null,
	load_src							varchar not null,
	primary key (h_transaction_pk, load_dt)
)
order by load_dt
segmented by h_transaction_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.s_transactions_amount(
	h_transaction_pk				bigint references STV2023121116__DWH.h_transactions(h_transaction_pk),
	currency_code					int not null,
	amount							int not null,
	load_dt							timestamp not null,
	load_src						varchar not null,
	primary key (h_transaction_pk, load_dt)
)
order by load_dt
segmented by h_transaction_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

create table STV2023121116__DWH.s_transactions_status(
	h_transaction_pk				bigint references STV2023121116__DWH.h_transactions(h_transaction_pk),
	status							varchar(30),
	transaction_dt					timestamp(3) not null,
	load_dt							timestamp not null,
	load_src						varchar not null,
	primary key (h_transaction_pk, load_dt)
)
order by load_dt
segmented by h_transaction_pk all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);













