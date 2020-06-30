set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_lasttrail_dealf_diff;

CREATE TABLE marketing_modeling.tmp_dlm_lasttrail_dealf_diff as 
SELECT 
	a1.mobile,
	a1.lasttrail_dealf_diff
FROM 
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.last_time) as lasttrail_dealf_diff	  --- 最后一次试乘试驾日期距最后成交时间差值
	FROM
		marketing_modeling.tmp_dlm_receive_processing a,
		marketing_modeling.tmp_dlm_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1
;