select customer_id
from {{ ref("customers") }}
where '__filter__.reg_date' is not null
    and '__filter__.ltv' is not null