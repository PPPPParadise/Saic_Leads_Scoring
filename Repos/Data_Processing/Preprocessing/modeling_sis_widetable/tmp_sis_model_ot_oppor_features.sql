set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--意向次数，意向品牌数，意向车型数，首次意向时间，最后意向时间
drop table if exists marketing_modeling.ot_oppor_features;
CREATE table marketing_modeling.ot_oppor_features as
SELECT 
    a.mobile as mobile,
	case when d.brand_id=121 then 'MG' when d.brand_id=101 then 'RW' else null end as brand,
    count(DISTINCT c.create_time) AS oppor_ttl,
    count(DISTINCT c.brand_id) as brand_count,
    count(DISTINCT c.vel_series_id) as oppor_series_count,
    min(c.create_time) AS fir_oppor_time,
    max(c.create_time) AS last_oppor_time,
    max(a.process_time) as process_time
from (select * from marketing_modeling.fail_outbound) a
LEFT JOIN (select distinct * from dtwarehouse.ods_dlm_t_cust_base where pt = '${pt}')b
on a.mobile = b.mobile
left join (SELECT DISTINCT * from dtwarehouse.ods_dlm_t_oppor where create_time is not null and pt = '${pt}')c
on b.id = c.cust_id
left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') d
on c.dealer_id = d.dlm_org_id     
where
a.process_time >= c.create_time
group by a.mobile,d.brand_id,process_time;
         