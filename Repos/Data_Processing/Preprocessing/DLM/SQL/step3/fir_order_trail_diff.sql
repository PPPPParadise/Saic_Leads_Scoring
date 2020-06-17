set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

--首次下定时间与首次到店时间的日期差值
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_fir_order_visit_diff;
CREATE TABLE marketing_modeling.dlm_tmp_fir_order_visit_diff as 
SELECT
	a.mobile, 
	datediff(b.deposit_order_time,a.fir_visit_time ) as fir_order_visit_diff	 
FROM
	marketing_modeling.dlm_tmp_oppor_processing2 b,
	marketing_modeling.dlm_tmp_hall_flow_processing a
WHERE
	a.mobile = b.mobile
;