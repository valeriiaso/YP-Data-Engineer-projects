-- 2.1 cdm.dm_courier_ledger

create table cdm.dm_courier_ledger(
	id serial not null,
	courier_id varchar not null,
	courier_name varchar not null,
	settlement_year int not null,
	settlement_month int not null check (settlement_month >= 1 and settlement_month <= 12),
	orders_count int not null default 0 check (orders_count >= 0),
	orders_total_sum numeric(14,2) not null default 0 check (orders_total_sum >= 0),
	rate_avg numeric(3, 2) not null check(rate_avg >= 1 and rate_avg <= 5),
	order_processing_fee numeric(14,2) not null default 0 check (order_processing_fee >= 0),
	courier_order_sum numeric(14,2) not null default 0 check (courier_order_sum >= 0),
	courier_tips_sum numeric(14,2) not null default 0 check (courier_tips_sum >= 0),
	courier_reward_sum numeric(14,2) not null default 0 check (courier_reward_sum >= 0),
	constraint dm_courier_ledger_pk primary key (id)
);

-- 2.2 струтктура DDS слоя

create table dds.dm_couriers( -- таблица, в которой хранятся данные о курьерах
	id serial not null,
	courier_id varchar not null,
	courier_name varchar not null,
	constraint dm_couriers_pk primary key(id)
);

alter table dds.dm_orders -- добавление столбца с идентификатором курьера в таблицу заказов
add column courier_id int not null;

update dds.dm_orders -- обновление таблицы dm_orders -> добавление идентификатора курьера
set courier_id = tbl.id
from (select	object_value::JSON->>'order_id' AS order_id,
		        object_value::JSON->>'courier_id' as courier_id,
		        dc.id
		from stg.deliveries del
		join dds.dm_couriers dc
		on object_value::JSON->>'courier_id' = dc.courier_id) tbl
where dm_orders.order_key = tbl.order_id;

alter table dds.dm_orders 
add constraint dn_orders_courier_id_fk foreign key (courier_id) references dds.dm_couriers(id);

create table dds.dm_deliveries( -- таблица, в которой хранятся данные о доставках
	id serial not null,
	order_id int not null,
	delivery_id varchar not null,
	address varchar not null,
	constraint dm_deliveries_pk primary key(id),
	constraint dm_deliveries_orders_id foreign key (order_id) references dds.dm_orders(id)
);


create table dds.fct_couriers_sale( -- таблица, в которой хранятся данные о курьерах и доставленных их заказах
	id serial not null,
	courier_id int not null,
	order_id int not null,
	rate int check (rate >= 1 and rate <= 5),
	order_sum numeric(14,2) not null default 0 check (order_sum >= 0),
	tips_sum numeric(14,2) not null default 0 check (tips_sum >= 0),
	constraint fct_couriers_sale_pk primary key(id),
	constraint fct_couriers_sale_orders_fk foreign key (order_id) references dds.dm_orders(id),
	constraint fct_couriers_sale_couriers_fk foreign key (courier_id) references dds.dm_couriers(id)
);

-- 2.3 структура STG слоя

create table stg.couriers(
	id serial not null,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint couriers_pk primary key (id)
);

alter table stg.couriers 
add constraint couriers_object_id_uindex unique (object_id);

create table stg.deliveries(
	id serial not null,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint deliveries_pk primary key (id)
);

alter table stg.deliveries 
add constraint deliveries_object_id_uindex unique (object_id);



