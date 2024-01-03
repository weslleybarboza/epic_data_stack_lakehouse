{{
    config(
        materialized='incremental'
    )
}}


WITH data_usage AS (
    SELECT *
    FROM {{ ref('stg_raw_pscore_sgw') }}
)
select
    case 
        when a.rec_type = 'SGWRecord' then 'data'
        when a.rec_type = 'SGSNPDPRecord' then 'data'
        when a.rec_type = 'SGSNPDPRecord' then 'data'
    end                                                                 as record_type,
    a.imsi                                                              as a_party_imsi,
    a.msisdn                                                            as a_party_msisdn,
    a.imei                                                              as a_party_imei,
  CAST(
    CONCAT(
      SUBSTR(a.record_opening_time, 1, 4), '-', 
      SUBSTR(a.record_opening_time, 5, 2), '-', 
      SUBSTR(a.record_opening_time, 7, 2), ' ', 
      SUBSTR(a.record_opening_time, 9, 2), ':', 
      SUBSTR(a.record_opening_time, 11, 2), ':', 
      SUBSTR(a.record_opening_time, 13, 2)
    ) 
    AS TIMESTAMP) as record_timestamp,
    --refined table where we can use incremental load
    a.*
from data_usage a
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where   CAST(
    CONCAT(
      SUBSTR(a.record_opening_time, 1, 4), '-', 
      SUBSTR(a.record_opening_time, 5, 2), '-', 
      SUBSTR(a.record_opening_time, 7, 2), ' ', 
      SUBSTR(a.record_opening_time, 9, 2), ':', 
      SUBSTR(a.record_opening_time, 11, 2), ':', 
      SUBSTR(a.record_opening_time, 13, 2)
    ) 
    AS TIMESTAMP) > (select max(  CAST(
    CONCAT(
      SUBSTR(a.record_opening_time, 1, 4), '-', 
      SUBSTR(a.record_opening_time, 5, 2), '-', 
      SUBSTR(a.record_opening_time, 7, 2), ' ', 
      SUBSTR(a.record_opening_time, 9, 2), ':', 
      SUBSTR(a.record_opening_time, 11, 2), ':', 
      SUBSTR(a.record_opening_time, 13, 2)
    ) 
    AS TIMESTAMP)) from {{ this }})

{% endif %}