set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--* 总活动次数,第一次参加活动
drop table if exists marketing_modeling.ot_activity_features;
create table marketing_modeling.ot_activity_features as 
select
    a.mobile, 
	case when d.brand_id=121 then 'MG' when d.brand_id=101 then 'RW' else null end as brand,
    a.call_rank,
    count(distinct c.create_time) as activity_ttl,
    min(attend_time) as fir_activity_time,
    max(a.process_time) as process_time
from 
(select mobile, process_time, call_rank from marketing_modeling.fail_outbound ) a
left join (select * from dtwarehouse.ods_dlm_t_cust_base where pt = '${pt}') b
on a.mobile = b.mobile
left join (select * from dtwarehouse.ods_dlm_t_cust_activity where pt = '${pt}') c
on b.id = c.cust_id
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') d
on c.dealer_id = d.dlm_org_id
where a.process_time >= c.create_time
group by a.mobile,d.brand_id,a.call_rank;