set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

--首次下定时间与首次试乘试驾的日期差值
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_fir_order_trail_diff;
CREATE TABLE marketing_modeling.dlm_tmp_fir_order_trail_diff as 
SELECT
	a.mobile, 
	datediff(b.deposit_order_time,a.fir_trail ) as fir_order_trail_diff	 
FROM
	marketing_modeling.dlm_tmp_oppor_processing2 b,
	marketing_modeling.dlm_tmp_receive_processing a
WHERE
	a.mobile = b.mobile
;