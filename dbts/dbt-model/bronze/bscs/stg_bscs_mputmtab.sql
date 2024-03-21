with source as (
      select * from {{ source('bscs', 'bscs_mputmtab') }}
),
renamed as (
    select
        {{ adapter.quote("tmcode") }},
        {{ adapter.quote("vscode") }},
        {{ adapter.quote("vsdate") }},
        {{ adapter.quote("status") }},
        {{ adapter.quote("tmind") }},
        {{ adapter.quote("des") }},
        {{ adapter.quote("shdes") }},
        {{ adapter.quote("plcode") }},
        {{ adapter.quote("plmnname") }},
        {{ adapter.quote("tmrc") }},
        {{ adapter.quote("rec_version") }},
        {{ adapter.quote("apdate") }},
        {{ adapter.quote("tmglobal") }},
        {{ adapter.quote("currency") }},
        {{ adapter.quote("global_iot_ind") }},
        {{ adapter.quote("dwh_etl_history_fk") }},
        {{ adapter.quote("flg_processed") }},
        {{ adapter.quote("flg_error") }},
        {{ adapter.quote("error_desc") }},
        {{ adapter.quote("stg_record_load_date") }}

    from source
)
select * from renamed