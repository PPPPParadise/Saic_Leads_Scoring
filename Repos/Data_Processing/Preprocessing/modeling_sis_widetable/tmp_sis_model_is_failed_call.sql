set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--是否战败外呼

drop table if exists marketing_modeling.ot_is_failed_call;

create table if not exists marketing_modeling.ot_is_failed_call as
WITH call_info_mas AS(
  select 
		mobile,
		call_time
  from  
		marketing_modeling.ods_manual_cultivation_call_info_mas 
  where 
        activity_name  like '%战败%' and
		call_time != '' and 
		call_time is not null
),manual_info AS (

  SELECT
        client_number,
		create_time
    FROM
        marketing_modeling.app_manual_sis_info
        WHERE second_source  like '%战败%'
		)
select 
	a.mobile,
	case when b.mobile is not null then 1 else 0 end as is_failed_call
	
from
	marketing_modeling.fail_outbound a
left join 
	(select mobile,call_time as create_time from call_info_mas union 
	 select client_number as mobile, create_time  from manual_info) b
on 
	a.mobile = b.mobile
where
	a.process_time >= b.create_time;