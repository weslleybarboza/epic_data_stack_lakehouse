with cust as (
    select
        cua.customer_id, 
        cua.custcode, 
        coa.co_id,  
        coa.co_activated,
        cua.customer_dealer,
        cua.csactivated,
        cca.ccfname,
        cca.cclname
    from {{ ref('bscs_customer_all') }} as cua
    left join {{ ref('bscs_contract_all') }} as coa on cua.customer_id = coa.customer_id
    left join {{ ref('bscs_ccontact_all') }} as cca on cua.customer_id = cca.customer_id
    where 1=1
    and cca.ccseq = '1'
)
, serv as (
    select
        co_id,
        cs_activ_date,
        cs_deactiv_date , 
        rank() over (partition by co_id order by cs_deactiv_date desc) as rank
    from {{ ref('bscs_contr_services_cap') }}
)
select
    cust.customer_id as natural_key
    ,cust.custcode as business_key
    ,cust.ccfname as first_name
    ,cust.cclname as last_name
    ,case when serv.cs_deactiv_date is null and serv.co_id is not null then 'Active' else 'Inactive' end as status
    ,'Y' as registered_flg
    ,null as registered_date
    ,cust.csactivated as date_from
    ,serv.cs_deactiv_date as c_date_to
    ,now() as rec_creation
	,now() as rec_update
   from cust as cust
   left join serv on cust.co_id = serv.co_id
where 1=1
and (serv.rank = 1 or serv.rank is null)