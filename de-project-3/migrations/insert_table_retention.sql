-- добавление информации в таблицу mart.f_customer_retention


delete from mart.f_customer_retention
where date_part('week', '{{ds}}'::date) = period_id
        and date_part('year', '{{ds}}'::date) = left(period_name, 4)::int;


insert into mart.f_customer_retention(new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
with weekly_orders as(
    select      week_of_year_iso as period_name,
                week_of_year as period_id,
                customer_id,
                "status",
  	        item_id,
                count(concat(fss.date_id, fss.item_id)) as count_order,
                sum(payment_amount) as total_revenue
    from mart.f_sales fss
    join mart.d_calendar cal
    on fss.date_id = cal.date_id
    where date_part('week', '{{ds}}'::date) = week_of_year
        and date_part('year', '{{ds}}'::date) = left(week_of_year_iso, 4)::int
    group by    customer_id,
                week_of_year_iso,
                week_of_year,
                "status",
  		item_id
)
select  sum(case
                when count_order = 1 and "status" != 'refunded' then 1
                else 0
                end) as new_customers_count,
        sum(case
                when count_order > 1 and "status" != 'refunded' then 1
                else 0
                end) as returning_customers_count,
        sum(case
                when "status" = 'refunded' then 1
                else 0
                end) as refunded_customer_count,
        period_name,
        period_id,
        item_id,
        sum(case
                when count_order = 1 and "status" != 'refunded' then total_revenue
                else 0
                end) as new_customers_revenue,
        sum(case
                when count_order > 1 and "status" != 'refunded' then total_revenue
                else 0
                end) as returning_customers_revenue,
        sum(case
                when "status" = 'refunded' then total_revenue
                else 0
                end) as customers_refunded
from weekly_orders
group by    period_name,
	        period_id,
		item_id;
