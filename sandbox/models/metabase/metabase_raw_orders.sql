select * 
from {{ ref('orders') }}
right join {{ ref("metabase_customer_filter")}} using(customer_id)
where '__filter__.order_date' is not null
    and '__filter__.status' is not null
    and '__filter__.amount' is not null