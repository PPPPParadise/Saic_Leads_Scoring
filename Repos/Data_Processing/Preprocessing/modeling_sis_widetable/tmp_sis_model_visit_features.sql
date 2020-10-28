set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--*总到店次数,首次到店时间,最后一次到店时间
drop table if exists marketing_modeling.ot_visit_features;
create table marketing_modeling.ot_visit_features as
select 
    a.mobile, 
	case when c.brand_id=121 then 'MG' when c.brand_id=101 then 'RW' else null end as brand,
    count(distinct b.create_time) as visit_ttl,
    min(b.arrive_time) as fir_visit_time,
    max(b.arrive_time) as last_visit_time,
    max(a.process_time) as process_time
from (select * from marketing_modeling.fail_outbound) a
left join (select * from dtwarehouse.ods_dlm_t_exhibition_hall_flow where pt = '${pt}') b
on a.mobile = b.customer_tel
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') c
on b.dealer_id = c.dlm_org_id
where
 a.process_time >= b.create_time
group by a.mobile,c.brand_id, a.call_rank;

