-- запрос для добавления нового стобца status в таблицы staging.user_order_log и mart.f_sales
-- для старых версий выгрузок, этот столбец по умолчанию принмает значение "shipped"

alter table staging.user_order_log
add column if not exists "status" varchar(20) default 'shipped';


alter table mart.f_sales
add column if not exists "status" varchar(20) default 'shipped';