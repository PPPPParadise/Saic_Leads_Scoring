set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--*试乘试驾总参与次数
drop table if exists marketing_modeling.ot_trail_features;
create table marketing_modeling.ot_trail_features as
select 
    a.mobile, 
	case when d.brand_id=121 then 'MG' when d.brand_id=101 then 'RW' else null end as brand,
    count(distinct c.create_time) as trail_ttl,
    max(a.process_time) as process_time
from (select mobile, process_time, call_rank from marketing_modeling.fail_outbound) a
left join (select * from dtwarehouse.ods_dlm_t_cust_base where pt = '${pt}') b
on a.mobile = b.mobile
left join (select * from dtwarehouse.ods_dlm_t_trial_receive where pt = '${pt}') c
on b.id = c.customer_id
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') d
on c.dealer_id = d.dlm_org_id
where c.status in ('14021003','14021004','14021005')
and a.process_time >= c.create_time
group by a.mobile,d.brand_id,a.process_time;
      