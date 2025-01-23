-- Шаг 2. Создание таблицы group_log в staging
drop table STV2023121116__STAGING.group_log

create table STV2023121116__STAGING.group_log(
	group_id	int,
	user_id		int,
	user_id_from	bigint,
	event		varchar(10),
	"datetime"	timestamp
)
order by group_id
partition by "datetime"::date
group by calendar_hierarchy_day("datetime"::date, 3, 2);



-- Шаг 4. Добавление таблицы l_user_group_activity в слой dwh
drop table STV2023121116__DWH.l_user_group_activity

create table STV2023121116__DWH.l_user_group_activity(
	hk_l_user_group_activity	bigint primary key,
	hk_user_id	bigint not null constraint fk_l_user_group_activity_users references STV2023121116__DWH.h_users(hk_user_id),
	hk_group_id	bigint not null constraint fk_l_user_group_activity_groups references STV2023121116__DWH.h_groups(hk_group_id),
	load_dt datetime,
	load_src	varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);


-- Шаг 5. Скрипты миграции в таблицу связи
insert into STV2023121116__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct 
				hash(hk_user_id, hk_group_id) as hk_l_user_group_activity,
				hk_user_id,
				hk_group_id,
				now() as load_dt,
				's3' as load_src
from STV2023121116__STAGING.group_log as gl
left join STV2023121116__DWH.h_users hu
on gl.user_id = hu.user_id
left join STV2023121116__DWH.h_groups hg
on gl.group_id = hg.group_id;


-- Шаг 6. Создание и наполнение сателлита
create table STV2023121116__DWH.s_auth_history(
	hk_l_user_group_activity	bigint not null constraint fk_auth_hist_user_group references STV2023121116__DWH.l_user_group_activity(hk_l_user_group_activity),
	user_id_from	int,
	event	varchar(10),
	event_dt	datetime,
	load_dt	datetime,
	load_src	varchar(20)
)
order by load_dt
segmented by hk_l_user_group_activity all nodes
partition by load_dt::date
group by calendar_hierarchy_day(load_dt::date, 3, 2);

insert into STV2023121116__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)
select	hk_l_user_group_activity,
		gl.user_id_from,
		gl.event,
		gl."datetime" as event_dt,
		now() as load_dt,
		's3' as load_scr
from STV2023121116__STAGING.group_log as gl
left join STV2023121116__DWH.h_groups as hg 
on gl.group_id = hg.group_id
left join STV2023121116__DWH.h_users as hu
on gl.user_id = hu.user_id
left join STV2023121116__DWH.l_user_group_activity as luga
on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;


-- Шаги 7

-- Шаг 7.1. Подготовка CTE user_group_messages
with user_group_messages as (
	select	luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
	from STV2023121116__DWH.l_user_group_activity luga
	join 
		(select hk_user_id,
				hk_group_id,
				count(distinct lum.hk_message_id) as count_messages
		from STV2023121116__DWH.l_user_message lum
		join STV2023121116__DWH.l_groups_dialogs lgd 
		on lum.hk_message_id = lgd.hk_message_id
		group by	hk_user_id,
					hk_group_id) as mes_count
	on mes_count.hk_user_id = luga.hk_user_id and mes_count.hk_group_id = luga.hk_group_id
	group by luga.hk_group_id)
	
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;

-- Шаг 7.2. Подготовка CTE user_group_log
with user_group_log as (
	select	luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_added_users
	from STV2023121116__DWH.l_user_group_activity luga
	join (
			select	hk_group_id,
					registration_dt
			from STV2023121116__DWH.h_groups hg
			order by registration_dt asc
			limit 10) as early_groups
	on early_groups.hk_group_id = luga.hk_group_id
	join STV2023121116__DWH.s_auth_history sah 
	on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
	where sah.event = 'add'
	group by luga.hk_group_id
)
select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10; 

-- Шаг 7.3. итоговый запрос
with user_group_messages as (
	select	luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
	from STV2023121116__DWH.l_user_group_activity luga
	join 
		(select hk_user_id,
				hk_group_id,
				count(distinct lum.hk_message_id) as count_messages
		from STV2023121116__DWH.l_user_message lum
		join STV2023121116__DWH.l_groups_dialogs lgd 
		on lum.hk_message_id = lgd.hk_message_id
		group by	hk_user_id,
					hk_group_id) as mes_count
	on mes_count.hk_user_id = luga.hk_user_id and mes_count.hk_group_id = luga.hk_group_id
	group by luga.hk_group_id),
	
	user_group_log as (
	select	luga.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_added_users
	from STV2023121116__DWH.l_user_group_activity luga
	join (
			select	hk_group_id,
					registration_dt
			from STV2023121116__DWH.h_groups hg
			order by registration_dt asc
			limit 10) as early_groups
	on early_groups.hk_group_id = luga.hk_group_id
	join STV2023121116__DWH.s_auth_history sah 
	on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
	where sah.event = 'add'
	group by luga.hk_group_id
)
select	ugl.hk_group_id,
		ugl.cnt_added_users,
		ugm.cnt_users_in_group_with_messages,
		ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm 
on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;
	






















