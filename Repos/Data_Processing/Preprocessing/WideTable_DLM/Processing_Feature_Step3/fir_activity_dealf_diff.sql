set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fir_activity_dealf_diff;
CREATE TABLE marketing_modeling.tmp_dlm_fir_activity_dealf_diff as 
SELECT 
	a1.mobile,
	--首参加活动时间距成交(战败)日期差值
	a1.fir_activity_dealf_diff
FROM
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.last_time ) as fir_activity_dealf_diff	  -- 首次参加活动时间距成交日期差值
	FROM
		marketing_modeling.tmp_dlm_cust_activity_processing a,
		marketing_modeling.tmp_dlm_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1 
;