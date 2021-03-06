set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

----首次下定时间与首次留资时间的日期差值   
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_fir_order_leads_diff;
CREATE TABLE marketing_modeling.dlm_tmp_fir_order_leads_diff as 
SELECT
	a.mobile, 
	datediff(b.deposit_order_time,a.fir_leads_time ) as fir_order_leads_diff	
FROM
	marketing_modeling.dlm_tmp_leads_processing a,
	marketing_modeling.dlm_tmp_oppor_processing2 b
WHERE
	a.mobile = b.mobile
;