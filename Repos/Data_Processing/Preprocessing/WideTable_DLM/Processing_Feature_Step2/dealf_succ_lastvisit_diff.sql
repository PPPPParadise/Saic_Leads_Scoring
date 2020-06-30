set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_dealf_succ_lastvisit_diff;
CREATE TABLE marketing_modeling.tmp_dlm_dealf_succ_lastvisit_diff as  
SELECT
	a1.mobile,
	--成交时间与成交经销商处最后一次到店时间日期差值
	round(sum(a1.last_buy_diff)/count(a1.dealer_id),2) as dealf_succ_lastvisit_diff
FROM
(
	SELECT
		a.mobile,
		a.dealer_id,
		a.last_time,
		b.fir_buy_time,
		datediff(b.fir_buy_time,a.last_time) as last_buy_diff
	FROM 
	(
		SELECT
			mobile,
			dealer_id,
			MAX(arrive_time) as last_time   	
		FROM 
			marketing_modeling.tmp_dlm_hall_flow_cleansing2 
		group by mobile,dealer_id 
	) a 
	left join 
	(
		select 
			mobile,
			dealer_id,
			min(buy_date) as fir_buy_time
		from 
			marketing_modeling.tmp_dlm_deliver_cleansing
		where buy_date is not null
		group by mobile,dealer_id
	) b on (a.mobile = b.mobile and a.dealer_id = b.dealer_id)
) a1 
where a1.dealer_id is not null 
and a1.fir_buy_time is not null
and a1.fir_buy_time >= a1.last_time
group by a1.mobile 
;

	
	
	