with source as (
    select * from {{ ref('stg_pscore_sgw') }}
    where 1=1
    {# and to_iso8601(rec_created) = to_iso8601(current_date) #}
)
, no_records as (
select
 date_format(rec_created, '%Y-%m-%d %H'),
 count(1) no_records
from source
group by 1
)
select * from no_records
where no_records < 100000