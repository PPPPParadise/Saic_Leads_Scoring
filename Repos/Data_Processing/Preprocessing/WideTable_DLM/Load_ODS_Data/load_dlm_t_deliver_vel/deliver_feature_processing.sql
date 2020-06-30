set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join = false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

--成交数据统计
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_deliver_processing;
CREATE TABLE marketing_modeling.tmp_dlm_deliver_processing as  
SELECT
    a.mobile,
	a.deal_time,    -- 首次成交时间
	a.last_time,    --最后成交时间   
	b.deal_car_ttl,   -- 两年内总购买车辆数
	b.deal_ttl     --总成交次数
FROM
	(
		SELECT
			mobile,
			first_time as deal_time,
			buy_date as last_time
		FROM
			marketing_modeling.tmp_dlm_deliver_cleansing2 
		WHERE rn = 1
	) a 
	left join 
	(
		SELECT 
			mobile,
			count(*) as deal_car_ttl,
			count(distinct buy_date) as deal_ttl     --成交次数
		FROM 
			marketing_modeling.tmp_dlm_deliver_cleansing2 
		 group by mobile 
	) b on a.mobile = b.mobile 
;
