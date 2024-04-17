-- abstracting the data source

with source_bscs as (
    select
        -- identification
        'BSCS'          as system_source,
        coal.co_id      as natural_key,
        -- contract header
        coal.co_id      as identification,
        case when coal.type = 'S' then 'Determinate' else 'Indeterminate' end         as type,
        case when coal.co_status is null then 'Inactive' else 'Active' end            as status,
        coal.customer_id    as customer_natural_key,
        ''              as date_signature,
        ''              as date_activation,
        ''              as date_cancelation,
        ''              as date_expiration,
        ''              as seller_natural_key,
        -- contract product
        ''              as product_natural_key
    from {{ ref('stg_dremio_bscs_contract_all') }} coal
    left join {{ ref('stg_dremio_bscs_contr_services_cap') }} cose on coal.co_id = cose.co_id
    {# left join {{ ref('model_name') }} #}
    where 1=1
    {# and coal.co_id = '399418' #}
)
select * 
from source_bscs