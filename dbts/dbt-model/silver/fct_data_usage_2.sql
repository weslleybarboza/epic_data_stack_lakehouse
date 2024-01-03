{{
    config(
        materialized='incremental'
    )
}}


WITH data_usage AS (
    SELECT *
    FROM {{ ref('stg_raw_pscore_sgw_2') }}
)
select
    case 
        when a.record_type = 'SGWRecord' then 'data'
        when a.record_type = 'SGSNPDPRecord' then 'data'
        when a.record_type = 'SGSNPDPRecord' then 'data'
    end                                                                 as record_type_refined,
    a.imsi                                                              as a_party_imsi,
    a.msisdn                                                            as a_party_msisdn,
    a.imei                                                              as a_party_imei,
    --refined table where we can use incremental load
    a.*
from data_usage a
where 1=1
{% if is_incremental() %}
  and a.event_date > coalesce((select max(event_date) from {{ this }}), CAST('1900-01-01' AS DATE))
{% else %}
  and 1=1
{% endif %}
