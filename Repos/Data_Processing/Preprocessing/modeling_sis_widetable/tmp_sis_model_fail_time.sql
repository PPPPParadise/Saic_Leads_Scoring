set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--* 战败次数,首次战败时间,最后战败时间
drop table if exists marketing_modeling.ot_fail_features;
create table marketing_modeling.ot_fail_features as
select 
    a.mobile, 
	a.brand,
    count(distinct b.cust_id) as fail_time,
    min(b.create_time) as fir_dealfail_d,
    max(b.create_time) as last_dealfail_d,
    max(a.process_time) as process_time
from
(select * from marketing_modeling.fail_outbound) a
left join
(
    select 
        b.mobile, 
        a.cust_id, 
		case when c.brand_id=121 then 'MG' when c.brand_id=101 then 'RW' else null end as brand,
        a.create_time
    from (select * from dtwarehouse.ods_dlm_t_oppor_fail where pt = '${pt}') a
    left join (select * from dtwarehouse.ods_dlm_t_cust_base where pt = '${pt}') b
	on a.cust_id = b.id
	left join (select * from dtwarehouse.ods_rdp_v_sales_region_dealer where pt = '${pt}') c
    on b.dealer_id = c.dlm_org_id 
) b
on a.mobile = b.mobile and a.brand=b.brand
where a.process_time >= b.create_time
group by a.mobile,a.brand,process_time;
