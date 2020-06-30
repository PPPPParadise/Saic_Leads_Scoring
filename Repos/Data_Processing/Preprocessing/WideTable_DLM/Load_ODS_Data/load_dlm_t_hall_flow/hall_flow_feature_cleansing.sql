set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

---- 到店信息清洗
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_hall_flow_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_hall_flow_cleansing as
select 
	a.customer_tel as mobile,
	a.dealer_id,
	a.arrive_time,
	a.arrive_count,
	ROW_NUMBER() OVER(PARTITION BY a.customer_tel ORDER BY a.arrive_time) AS rn  
from 
	dtwarehouse.Ods_dlm_t_exhibition_hall_flow a,
	marketing_modeling.tmp_dlm_cust_cleansing b 
where 
	pt = '${pt}'
	and a.create_time >= '${beginTime}' 
	AND length(replace(a.customer_tel,'+86','')) = 11
	and a.customer_tel = b.mobile 
	and a.dealer_id = b.dealer_id 
;
  

---- 到店按经销商分组
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_hall_flow_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_hall_flow_cleansing2 as
select 
	mobile,
	dealer_id,
	arrive_time,
	ROW_NUMBER() OVER(PARTITION BY mobile,dealer_id ORDER BY arrive_time) AS rn2  
from 
	marketing_modeling.tmp_dlm_hall_flow_cleansing 
;