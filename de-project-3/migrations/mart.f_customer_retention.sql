-- скрипт для создания таблицы mart.f_customer_retention

create table if not exists mart.f_customer_retention(
  new_customers_count		int,
  returning_customers_count	int,
  refunded_customer_count	int,
  period_name                   varchar(30),
  period_id			int,
  item_id		        int,
  new_customers_revenue         real,
  returning_customers_revenue   real,
  customers_refunded            int,
  primary key(period_id, item_id)
);

