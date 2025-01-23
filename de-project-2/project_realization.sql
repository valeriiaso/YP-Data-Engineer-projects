-- Step 1

--drop table if exists public.shipping_country_rates;

create table public.shipping_country_rates(
	id							serial,
	shipping_country			varchar(30),
	shipping_country_base_rate	numeric(14,3),
	primary key (id)
);

insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct 
		shipping_country,
		shipping_country_base_rate
from public.shipping;


-- Step 2

--drop table if exists public.shipping_agreement;

create table public.shipping_agreement(
	agreement_id			bigint,
	agreement_number		varchar(20),
	agreement_rate			numeric(14,3),
	agreement_commission	numeric(14,3),
	primary key (agreement_id)
);


insert into public.shipping_agreement
(agreement_id, agreement_number, agreement_rate, agreement_commission)
select distinct
		cast(agreement_id as bigint) as agreement_id,
		agreement_number,
		cast(agreement_rate as numeric(14,3)) as agreement_rate,
		cast(agreement_commission as numeric(14,3)) as agreement_commission
from
	(
		select	(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1] as agreement_id,
				(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[2] as agreement_number,
				(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[3] as agreement_rate,
				(regexp_split_to_array(vendor_agreement_description, E'\\:+'))[4] as agreement_commission
		from public.shipping
	) as tab;


-- Step 3

--drop table if exists public.shipping_transfer;

create table public.shipping_transfer(
	id						serial,
	transfer_type			varchar(5),
	transfer_model			varchar(20),
	shipping_transfer_rate	numeric(14,3),
	primary key (id)
);


insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select distinct
		(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[1] as transfer_type,
		(regexp_split_to_array(shipping_transfer_description, E'\\:+'))[2] as transfer_model,
		shipping_transfer_rate
from
	(
		select distinct 
				shipping_transfer_description,
				shipping_transfer_rate
		from public.shipping
	) as tab;


-- Step 4

--drop table if exists public.shipping_info;

create table public.shipping_info(
	shipping_id					bigint,
	payment_amount				numeric(14,2),
	vendor_id					bigint,
	shipping_plan_datetime		timestamp,
	shipping_country_rate_id	bigint,
	shipping_agreement_id		bigint,
	shipping_transfer_id		bigint,
	primary key (shipping_id),
	foreign key (shipping_country_rate_id) references public.shipping_country_rates(id),
	foreign key (shipping_agreement_id) references public.shipping_agreement(agreement_id),
	foreign key (shipping_transfer_id) references public.shipping_transfer(id)
);


insert into public.shipping_info -- изменено условие JOIN'а для таблицы shipping_agreement
(shipping_id, payment_amount, vendor_id, shipping_plan_datetime, shipping_country_rate_id, shipping_agreement_id, shipping_transfer_id)
select distinct
		shipping_id,
		payment_amount,
		vendor_id,
		shipping_plan_datetime,
		sr.id as shipping_country_rate_id,
		sa.agreement_id as shipping_agreement_id,
		st.id as shipping_transfer_id
from public.shipping sp
left join public.shipping_country_rates sr
on sp.shipping_country = sr.shipping_country
join public.shipping_agreement sa
on cast((regexp_split_to_array(vendor_agreement_description, E'\\:+'))[1] as bigint) = agreement_id
join (
		select	id,
				concat(transfer_type, ':', transfer_model)
		from public.shipping_transfer
) as st
on sp.shipping_transfer_description = st.concat;


-- Step 5

--drop table if exists public.shipping_status;

create table public.shipping_status(
	shipping_id						bigint,
	status							varchar(15),
	state							varchar(15),
	shipping_start_fact_datetime	timestamp,
	shipping_end_fact_datetime		timestamp,
	primary key (shipping_id)
);


insert into public.shipping_status -- изменена логика сборки таблицы
(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select	start_fin_ship.shipping_id,
		status,
		state,
		start_ship as shipping_start_fact_datetime,
		(case  
			when state = 'recieved' or state = 'returned' then fin_ship 
			else null 
		end) as shipping_end_fact_datetime
from 	
(
	select	shipping_id, 
			min(state_datetime) as start_ship, 
			max(state_datetime) as fin_ship
	from shipping s 
	where state != 'returned'
	group by shipping_id
) as start_fin_ship
join 
(
	select	shipping_id,
			state,
			status,
			state_datetime,
			row_number() over(partition by shipping_id order by state_datetime desc) as row_num
	from shipping
) as stat_ship
on start_fin_ship.shipping_id = stat_ship.shipping_id
where stat_ship.row_num = 1;


-- Step 6

--drop table if exists public.shipping_datamart;

create table public.shipping_datamart(
	shipping_id				bigint,
	vendor_id				bigint,
	transfer_type			varchar(5),
	full_day_at_shipping	int,
	is_delay				bool,
	is_shipping_finish		bool,
	delay_day_at_shipping	int,
	payment_amount			numeric(14,2),
	vat						numeric(14,3),
	profit					numeric(14,3),
	primary key (shipping_id)
);

insert into public.shipping_datamart
(shipping_id, vendor_id, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, payment_amount, vat, profit)
select	distinct 
			si.shipping_id,
			si.vendor_id,
			st.transfer_type,
			date_part('day', shipping_end_fact_datetime - shipping_start_fact_datetime) as full_day_at_shipping,
			(case
				when shipping_end_fact_datetime > shipping_plan_datetime then true
				else false
			end
			) as is_delay,
			(case
				when status = 'finished' then true
				else false
			end
			) as is_shipping_finish,
			(case
				when shipping_end_fact_datetime > shipping_plan_datetime
				then date_part('day', shipping_end_fact_datetime - shipping_plan_datetime)
				else 0
			end
			) as delay_day_at_shipping,
			payment_amount,
			(payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate)) as vat,
			(payment_amount * agreement_commission) as profit
from shipping_info si
join shipping_transfer st 
on si.shipping_transfer_id = st.id
join (
		select	shipping_id,
				status,
				state,
				shipping_start_fact_datetime,
				(case
					when shipping_end_fact_datetime is not null then shipping_end_fact_datetime
					else current_timestamp
				end
				) as shipping_end_fact_datetime
		from shipping_status) as ss
on si.shipping_id = ss.shipping_id 
join public.shipping_agreement sa
on si.shipping_agreement_id = sa.agreement_id 
join public.shipping_country_rates sc
on si.shipping_country_rate_id = sc.id;


