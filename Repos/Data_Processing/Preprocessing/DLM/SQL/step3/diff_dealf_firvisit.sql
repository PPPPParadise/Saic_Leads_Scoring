set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_dealf_firvisit_diff;

CREATE TABLE marketing_modeling.dlm_tmp_dealf_firvisit_diff as 
SELECT 
	a1.mobile,
	-- if(a1.mobile is null,a2.mobile,a1.mobile) as mobile,
	a1.dealf_lastvisit_diff,
	a1.dealf_firvisit_diff
FROM 
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.last_time) as dealf_lastvisit_diff,	-- 成交时间与全经销商处最后一次到店时间日期差值
		datediff(b.last_time,a.fir_visit_time) as dealf_firvisit_diff	  --- 成交时间与全经销商处首次到店时间日期差值
	FROM
		marketing_modeling.dlm_tmp_hall_flow_processing a,
		marketing_modeling.dlm_tmp_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.last_time
) a1
;	