with source as (
      select * from {{ source('bscs', 'bscs_mpusptab') }}
),
renamed as (
    select
        {{ adapter.quote("spcode") }},
        {{ adapter.quote("des") }},
        {{ adapter.quote("shdes") }},
        {{ adapter.quote("sptype") }},
        {{ adapter.quote("rec_version") }},
        {{ adapter.quote("dwh_etl_history_fk") }},
        {{ adapter.quote("flg_processed") }},
        {{ adapter.quote("flg_error") }},
        {{ adapter.quote("error_desc") }},
        {{ adapter.quote("stg_record_load_date") }}

    from source
)
select * from renamed
  