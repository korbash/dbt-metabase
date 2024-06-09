select 
    status,
    sum(amount) as sum,
    count(distinct customer_id) as users,
    count(distinct order_id) as deals
from {{ ref('orders') }}
where '__filter__.order_date' is not null
group by status
order by status