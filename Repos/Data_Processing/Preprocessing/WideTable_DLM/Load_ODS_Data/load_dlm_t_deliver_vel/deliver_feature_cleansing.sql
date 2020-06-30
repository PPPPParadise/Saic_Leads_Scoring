---- 成交信息清洗
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_deliver_cleansing;

CREATE TABLE marketing_modeling.tmp_dlm_deliver_cleansing as
select 
	a.cust_id,
	b.mobile,
	a.dealer_id,
	a.brand_id,
	a.vel_series_id,
	a.vel_model_id,
	if(a.buy_date is null , a.create_time,concat(a.buy_date,' 00:00:00')) as buy_date,
	a.oppor_id 
from 
    (
		select * from dtwarehouse.ods_dlm_t_deliver_vel 
		where 
			pt = '${pt}'
		and create_time >= '${beginTime}' 
		and cust_id is not null 
	) a ,
	marketing_modeling.tmp_dlm_cust_cleansing b 
where
	a.cust_id = b.cust_id and a.dealer_id = b.dealer_id 
;

-- 按mobile再分组计算出首次成交时间 
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_deliver_cleansing2;
CREATE TABLE marketing_modeling.tmp_dlm_deliver_cleansing2 as
select 
	a.*,
	MIN(a.buy_date) OVER(PARTITION BY a.mobile) as first_time,
	ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.buy_date desc) AS rn	
from 
	marketing_modeling.tmp_dlm_deliver_cleansing a