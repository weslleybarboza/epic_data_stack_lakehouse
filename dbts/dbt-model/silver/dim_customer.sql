with raw as (
    select * 
    from {{ ref('bscs_customer_all') }} as cua
    left join {{ ref('bscs_contract_all') }} as coa on cua.customer_id = coa.customer_id
    left join {{ ref('bscs_ccontact_all') }} as cca on cua.customer_id = cca.customer_id
    where 1=1
    and cca.ccseq = 1
)
, service as (
    select
        co_id,
        cs_activ_date,
        cs_deactiv_date , 
        rank() over (partition by co_id order by cs_deactiv_date desc)
    from {{ ref('bscs_contr_services_cap') }}
)
select
        rw.customer_id, 
        rw.custcode, 
        rw.co_id,  
        rw.co_activated 
        rw.c_activation_date, 
        rw.customer_dealer, 
        rw.csactivated
    from raw as rw
    left join service sr on rw.co_id and sr.co_id
where 1=1
and (sr.rank = 1 or sr.rank is null)