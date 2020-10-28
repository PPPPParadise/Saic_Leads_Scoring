set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--*首次建卡时间, 总建卡次数, 战败前意向等级 
drop table if exists marketing_modeling.ot_cust_features;
create table marketing_modeling.ot_cust_features as 
select 
    a.mobile, 
	case when c.brand_id=121 then 'MG' when c.brand_id=101 then 'RW' else null end as brand,
    count(distinct id) as card_ttl,
    count(distinct dealer_id) as leads_dtbt_count,
    min(b.create_time) as first_card_time,
	max(b.create_time) as last_card_time,
    min(b.cust_level) as highest_cust_level,
    max(b.cust_level) as lowest_cust_level,
    max(a.process_time) as process_time
from 
(select * from marketing_modeling.fail_outbound) a
left join (select * from dtwarehouse.ods_dlm_t_cust_base where pt = '${pt}') b
on a.mobile = b.mobile
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') c
on b.dealer_id = c.dlm_org_id
where  a.process_time >= b.create_time
group by a.mobile, c.brand_id,process_time;
