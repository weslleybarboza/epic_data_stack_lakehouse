with all_customer as (
    select * from {{ ref('dim_customer') }}
)
select
    status,
    count(1) as no_customers
from all_customer
group by 1