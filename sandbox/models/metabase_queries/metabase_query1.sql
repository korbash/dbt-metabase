select * from {{ ref('orders') }}
where '__filter__.order_date' is not null
    and '__filter__.status' is not null
    and '__filter__.amount' is not null