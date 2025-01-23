drop table STV2023121116__STAGING.transactions

-- transactions
create table if not exists STV2023121116__STAGING.transactions(
	operation_id		varchar(60) not null,
	account_number_from	int,
	account_number_to	int,
	currency_code		int,
	country				varchar(30),
	status				varchar(30),
	transaction_type	varchar(30),
	amount				int,
	transaction_dt		timestamp(3)
)
order by transaction_dt, operation_id
segmented by hash(transaction_dt, operation_id) all nodes ksafe 1
partition by transaction_dt::date 
group by calendar_hierarchy_day(transaction_dt::date, 3, 2);


-- currencies
create table if not exists STV2023121116__STAGING.currencies(
	id					identity(1, 1),
	date_update			timestamp(3),
	currency_code		int,
	currency_code_with	int,
	currency_with_div	numeric(5, 3)
	
)
order by date_update, id
segmented by hash(date_update, id) all nodes ksafe 1
partition by date_update::date
group by calendar_hierarchy_day(date_update::date, 3, 2);








