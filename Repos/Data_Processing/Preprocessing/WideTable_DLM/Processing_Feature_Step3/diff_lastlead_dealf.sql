set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_lastlead_dealf_diff;

CREATE TABLE marketing_modeling.tmp_dlm_lastlead_dealf_diff as 
SELECT 
	a1.mobile,
	a1.lastlead_dealf_diff
FROM
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.last_time ) as lastlead_dealf_diff	  -- 最后一次留资时间距最后成交时间差值
	FROM
		marketing_modeling.tmp_dlm_leads_processing a,
		marketing_modeling.tmp_dlm_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1 
;