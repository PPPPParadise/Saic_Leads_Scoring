set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_task_item_cleansing_1;
CREATE TABLE marketing_modeling.tmp_dlm_task_item_cleansing_1 as 
with tk as (
    select id 
    from dtwarehouse.ods_smcsc_tsk_task a 
    where pt='${pt}' 
      and a.task_type = '001'
      and a.created_time >= '${beginTime}'
) ,
t1 as (
   select id,
        task_item_id,
        ob_result_code,
        ob_count,
        call_id,
		connid,
        is_sued,
        created_time,
		ROW_NUMBER() OVER(PARTITION BY task_item_id,call_id ORDER BY created_time desc) AS rn
    FROM 
        dtwarehouse.ods_smcsc_tsk_task_item_process 
    where pt='${pt}' and call_id is not null and created_time >= '${beginTime}'
),
t2 as (
    select id,
		task_id,
		cust_id
    from 
        dtwarehouse.ods_smcsc_tsk_task_item 
    where pt='${pt}' and created_time >= '${beginTime}'
),
t3 as (
    select id,
		mobile_no
    from 
        dtwarehouse.ods_smcsc_tsk_customer 
    where pt='${pt}' and create_time >= '${beginTime}'
),
t4 as (
	select id,
		task_item_id,
		brand
    from 
        dtwarehouse.ods_smcsc_tsk_task_item_ext 
    where pt='${pt}' and created_time >= '${beginTime}'
)
select 
    t2.task_id,
    t2.cust_id,
    t3.mobile_no as mobile,
    t1.id,
    t1.task_item_id,
    t1.ob_result_code,
    t1.ob_count,
    t1.call_id,
    t1.connid,
    t1.is_sued,
    t1.created_time
from 
    t1,t2,t3,t4,tk
where t1.rn = 1
  and t1.task_item_id = t2.id
  and t2.task_id = tk.id
  and t2.cust_id = t3.id
  and t2.id = t4.task_item_id
  and t4.brand = 'MG'
;