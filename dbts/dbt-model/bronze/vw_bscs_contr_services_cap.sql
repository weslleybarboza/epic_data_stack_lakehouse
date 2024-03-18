with source as (
      select * from {{ source('bronze', 'bscs_contr_services_cap') }}
),
renamed as (
    select
        {{ adapter.quote("co_id") }},
        {{ adapter.quote("sncode") }},
        {{ adapter.quote("seqno") }},
        {{ adapter.quote("seqno_pre") }},
        {{ adapter.quote("bccode") }},
        {{ adapter.quote("pending_bccode") }},
        {{ adapter.quote("dn_id") }},
        {{ adapter.quote("main_dirnum") }},
        {{ adapter.quote("cs_status") }},
        {{ adapter.quote("cs_activ_date") }},
        {{ adapter.quote("cs_deactiv_date") }},
        {{ adapter.quote("cs_request") }},
        {{ adapter.quote("rec_version") }},
        {{ adapter.quote("dn_block_id") }},
        {{ adapter.quote("dwh_etl_history_fk") }},
        {{ adapter.quote("flg_processed") }},
        {{ adapter.quote("flg_error") }},
        {{ adapter.quote("error_desc") }},
        {{ adapter.quote("stg_record_load_date") }}

    from source
)
select * from renamed
