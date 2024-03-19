with data_activity as (
    select *
    from {{ ref('stg_pscore_sgw') }}
)
select
    da.imsi                      as imsi_part_a
    ,da.msisdn                   as msisdn_part_a
    ,da.local_sequence_number    as id_dim_local
    ,da.volume_uplink            as volume_upload
    ,da.volume_downlink          as volume_download
    ,da.total_volume             as volume_total
    ,da.lac_or_tac               as id_dim_antena
    ,da.mccmnc                   as id_dim_operator
    ,da.output_filename          as cdr_file_name
    {# ,da.* #}
from data_activity da