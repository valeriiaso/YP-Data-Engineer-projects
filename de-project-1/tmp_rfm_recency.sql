insert into analysis.tmp_rfm_recency
with recency_calc as (
  select 	user_id, 
		  cast((case
			   	when status = 4 then order_ts
			   	else '2021-01-01'
			   	end) as date) as order_ts
  from analysis.users u
  left join analysis.orders o
  on u.id = o.user_id
  where extract(year from order_ts) >= 2022
)
select 	user_id,
		ntile(5) over(order by max(order_ts) asc) as recency
from recency_calc
group by user_id;
