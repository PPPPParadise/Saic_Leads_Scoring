set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

---- 试乘信息清洗

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_trial_receive_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_trial_receive_cleansing as
SELECT 
	a.customer_id as cust_id,
	b.mobile as mobile,
	a.reservation_time, --	预约时间
	a.trial_time,       --	试驾时间
	a.status,
	a.vehicle_series,    --试驾车系
	a.vehicle_model,     --试驾车型
	a.dealer_id,
	ROW_NUMBER() OVER(PARTITION BY a.customer_id ORDER BY a.trial_time desc) AS rn	
FROM 
	(
		select * 
		from dtwarehouse.ods_dlm_t_trial_receive 
		WHERE pt = '${pt}'
		  and create_time >= '${beginTime}'
	) a,           -- 试乘表
	marketing_modeling.tmp_dlm_cust_cleansing b 
where a.customer_id = b.cust_id and a.dealer_id = b.dealer_id 
;

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_trial_receive_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_trial_receive_cleansing2 as
select 
	a.*,
	max(reservation_time) OVER(PARTITION BY a.mobile) as last_reservation_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.trial_time desc) AS rn2	
from 
	marketing_modeling.tmp_dlm_trial_receive_cleansing a
where a.mobile is not null;
