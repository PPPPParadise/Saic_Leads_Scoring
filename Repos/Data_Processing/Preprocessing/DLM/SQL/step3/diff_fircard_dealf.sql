set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_fircard_dealf_diff;

CREATE TABLE marketing_modeling.dlm_tmp_fircard_dealf_diff as 
SELECT 
	a1.mobile,
	a1.fircard_dealf_diff
FROM
(
	SELECT
		a.mobile, 
		datediff(b.last_time,a.fir_card_time) as fircard_dealf_diff	  --- 首次建卡时间距最后成交时间差值
	FROM
		marketing_modeling.dlm_tmp_cust_processing a,
		marketing_modeling.dlm_tmp_last_deal_time b
	WHERE
		a.mobile = b.mobile and b.last_time > a.fir_card_time
) a1 
;
