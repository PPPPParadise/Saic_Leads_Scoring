set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

-- 建卡用户意向车型数统计
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_oppor_processing;
CREATE TABLE marketing_modeling.tmp_dlm_oppor_processing as  
SELECT
	mobile,
	series_brand_id,                 -- 121 代表MG  101代表RW
	count(distinct series_type) as leads_car_model_type,     --留资车型种类数
	collect_set(vel_series_id) as vel_series_ids,
	collect_set(series_type) as series_types,
	count(distinct vel_series_id) as leads_car_model_count
from  
	marketing_modeling.tmp_dlm_oppor_cleansing 
where mobile is not null 
group by mobile,series_brand_id;

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_oppor_processing2;
CREATE TABLE marketing_modeling.tmp_dlm_oppor_processing2 as  
SELECT
	mobile,
	min(deposit_order_create_time) as deposit_order_time,       -- 下定时间
	if(sum(is_deposit_order)>=1,1,0)  as is_deposit_order        --是否下定 

from  
	marketing_modeling.tmp_dlm_oppor_cleansing 
where mobile is not null 
group by mobile;