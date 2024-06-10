select 
    order_date::date as d,
    sum(amount) as sum,
    count(distinct customer_id) as users,
    count(distinct order_id) as deals
from {{ ref('orders') }}
where '__filter__.order_date' is not null
    and '__filter__.amount' is not null
group by d, status
order by d, status