set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--*线索来源,留资经销商数量,总留资次数,留资车型数量
drop table if exists marketing_modeling.ot_leads_source;
create table marketing_modeling.ot_leads_source as
select 
    a.mobile, 
	a.call_rank,
	case when c.brand_id=121 then 'MG' when c.brand_id=101 then 'RW' else a.brand end as brand,
    first_resource_name, 
    second_resource_name,
    b.create_time as lead_time,
    b.data_source_from_website as channel,
	b.dealer_id,
    rank() over(partition by a.mobile, a.brand order by b.create_time desc) as leads_rank,
    a.process_time as process_time
from
(select * from marketing_modeling.fail_outbound) a
left join (select * from dtwarehouse.ods_dlm_t_potential_customer_leads_m where pt = '${pt}')b
on a.mobile = b.mobile
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}')c
on b.dealer_id = c.dlm_org_id
where
	a.process_time >= b.create_time 
	;

drop table if exists marketing_modeling.ot_leads_features;
create table marketing_modeling.ot_leads_features as
SELECT 
    a.mobile as mobile,
	a.brand,
    max(b.first_resource_name) as first_resource_name,
    max(b.second_resource_name) as second_resource_name,
    a.process_time as process_time,
    b.lead_time as last_leads_time,
    a.first_resource_count as first_resource_count,
    a.second_resource_count as second_resource_count,
    a.leads_count as leads_count,
	a.dealer_count
FROM (
    SELECT 
    mobile,
	brand,
    count(DISTINCT(first_resource_name)) as first_resource_count,
    count(DISTINCT(second_resource_name)) as second_resource_count,
    count(DISTINCT(lead_time)) as leads_count,
	count(DISTINCT(dealer_id)) as dealer_count,
    process_time
    FROM marketing_modeling.ot_leads_source
    GROUP BY mobile,brand,process_time
) a
LEFT JOIN (select distinct * from marketing_modeling.ot_leads_source where leads_rank = 1) b
ON a.mobile = b.mobile and a.process_time = b.process_time
GROUP BY a.mobile,a.brand,a.process_time, a.first_resource_count, 
a.second_resource_count, a.leads_count, b.lead_time,a.dealer_count
