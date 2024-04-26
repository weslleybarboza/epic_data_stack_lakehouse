with source as (
      select * from {{ source('pscore', 'pscore_sgw') }}
),
renamed as (
    select
        {{ adapter.quote("record_type") }},
        {{ adapter.quote("network_initiated_pdp_context") }},
        {{ adapter.quote("imsi") }},
        {{ adapter.quote("msisdn") }},
        {{ adapter.quote("imei") }},
        {{ adapter.quote("charging_id") }},
        {{ adapter.quote("ggsn_pgw_address") }},
        {{ adapter.quote("sgsn_sgw_address") }},
        {{ adapter.quote("ms_nw_capability") }},
        {{ adapter.quote("pdp_pdn_type") }},
        {{ adapter.quote("served_pdp_address") }},
        {{ adapter.quote("dynamic_address_flag") }},
        {{ adapter.quote("access_point_name_ni") }},
        {{ adapter.quote("record_sequence_number") }},
        {{ adapter.quote("record_sequence_number_meg") }},
        {{ adapter.quote("node_id") }},
        {{ adapter.quote("local_sequence_number") }},
        {{ adapter.quote("charging_characteristics") }},
        {{ adapter.quote("record_opening_time") }},
        {{ adapter.quote("duration") }},
        {{ adapter.quote("rat_type") }},
        {{ adapter.quote("cause_for_record_closing") }},
        {{ adapter.quote("diagnostic") }},
        {{ adapter.quote("volume_uplink") }},
        {{ adapter.quote("volume_downlink") }},
        {{ adapter.quote("total_volume") }},
        {{ adapter.quote("lac_or_tac") }},
        {{ adapter.quote("ci_or_eci") }},
        {{ adapter.quote("rac") }},
        {{ adapter.quote("rnc_unsent_data_volume") }},
        {{ adapter.quote("req_alloc_ret_priority") }},
        {{ adapter.quote("neg_alloc_ret_priority") }},
        {{ adapter.quote("req_traffic_class") }},
        {{ adapter.quote("neg_traffic_class") }},
        {{ adapter.quote("qci") }},
        {{ adapter.quote("req_max_bitrate_uplink") }},
        {{ adapter.quote("req_max_bitrate_downlink") }},
        {{ adapter.quote("req_guar_bitrate_uplink") }},
        {{ adapter.quote("req_guar_bitrate_downlink") }},
        {{ adapter.quote("neg_max_bitrate_uplink") }},
        {{ adapter.quote("neg_max_bitrate_downlink") }},
        {{ adapter.quote("neg_guar_bitrate_uplink") }},
        {{ adapter.quote("neg_guar_bitrate_downlink") }},
        {{ adapter.quote("mccmnc") }},
        {{ adapter.quote("country_name") }},
        {{ adapter.quote("input_filename") }},
        {{ adapter.quote("output_filename") }},
        {{ adapter.quote("event_date") }},
        {{ adapter.quote("rec_created") }},
        {{ adapter.quote("rec_updated") }}

    from source
)
select * from renamed
  