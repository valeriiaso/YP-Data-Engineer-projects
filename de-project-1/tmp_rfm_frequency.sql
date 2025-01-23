insert into analysis.tmp_rfm_frequency
with frequency_calc as (
  select 	user_id,
  			sum(case
				  when status = 4 then 1
				  else 0
				  end) as count_orders
  from users u
  left join orders o
  on u.id = o.user_id
  where extract(year from order_ts) >= 2022
  group by user_id
 )
select 	user_id,
		ntile(5) over(order by count_orders asc) as frequency
from frequency_calc
