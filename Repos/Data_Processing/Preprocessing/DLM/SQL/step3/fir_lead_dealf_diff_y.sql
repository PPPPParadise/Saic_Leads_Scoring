set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_fir_leads_deal_diff_y;

CREATE TABLE marketing_modeling.dlm_tmp_fir_leads_deal_diff_y as 
SELECT
	a.mobile, 
	round(months_between(a.fir_leads_time,b.bought_time)/12,2) as fir_leads_deal_diff_y	  -- 上次成交时间距首次留资时间年数
FROM
	marketing_modeling.dlm_tmp_leads_processing a,
	(
		select mobile, max(bought_time) as bought_time 
		from marketing_modeling.dlm_tmp_vehicle_cleansing
		group by mobile 
	) b
WHERE
	a.mobile = b.mobile 
;