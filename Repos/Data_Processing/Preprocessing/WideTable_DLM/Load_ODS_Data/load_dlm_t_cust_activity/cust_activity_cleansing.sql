set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};
---- 客户活动信息表清洗

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_cust_activity_cleansing;
CREATE TABLE marketing_modeling.tmp_dlm_cust_activity_cleansing as
SELECT 
	a.cust_id,
	b.mobile,
	a.dealer_id,
	a.attend_time,
	a.create_time
FROM 
	(
		select 
			cust_id,
			dealer_id,
			source_id,  --来源编码（客户、意向、跟进）
			attend_time,  --参与时间
			invite_time,
			create_time
		from
			dtwarehouse.ods_dlm_t_cust_activity          
		WHERE create_time >= '${beginTime}' 
		  AND cust_id is not null 
		  and pt = '${pt}'
	) a,
	marketing_modeling.tmp_dlm_cust_cleansing b 
where a.cust_id = b.cust_id and a.dealer_id = b.dealer_id 
;

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_cust_activity_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_cust_activity_cleansing2 as
select 
	a.*,
	min(attend_time) OVER(PARTITION BY a.mobile) as first_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.attend_time desc) AS rn	
from 
	marketing_modeling.tmp_dlm_cust_activity_cleansing a
where a.mobile is not null;  
