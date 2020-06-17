set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_firfollow_dealf_diff;

CREATE TABLE marketing_modeling.dlm_tmp_firfollow_dealf_diff as 
SELECT 
	a1.mobile,
	a1.firfollow_dealf_diff,
	a1.lastfollow_dealf_diff
FROM 
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.first_time) as firfollow_dealf_diff,   --首次跟进日期距成交时间差值
		datediff(b.last_time,a.last_time) as lastfollow_dealf_diff	  --- 最后一次跟进日期距成交日期差值
	FROM
		marketing_modeling.dlm_tmp_followup_processing a,
		marketing_modeling.dlm_tmp_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1
;	