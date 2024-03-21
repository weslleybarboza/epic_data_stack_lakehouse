with df as (
    select * from {{ ref('dim_customer') }}
)
select *
from df 
where registered_date is null or registered_date = ''