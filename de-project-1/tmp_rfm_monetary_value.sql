insert into analysis.tmp_rfm_monetary_value
with frequency_calc as (
  select 	user_id,
  			sum(case
				  when status = 4 then payment
				  else 0
				  end) as sum_orders
  from users u
  left join orders o
  on u.id = o.user_id
  where extract(year from order_ts) >= 2022
  group by user_id
 )
select 	user_id,
		ntile(5) over(order by sum_orders asc) as monetary_value
from frequency_calc;