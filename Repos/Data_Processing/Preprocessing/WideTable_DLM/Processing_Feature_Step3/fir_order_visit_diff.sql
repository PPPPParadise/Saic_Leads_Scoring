set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

--首次下定时间与首次试乘试驾的日期差值
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fir_order_trail_diff;
CREATE TABLE marketing_modeling.tmp_dlm_fir_order_trail_diff as 
SELECT
	a.mobile, 
	datediff(b.deposit_order_time,a.fir_trail ) as fir_order_trail_diff	 
FROM
	marketing_modeling.tmp_dlm_oppor_processing2 b,
	marketing_modeling.tmp_dlm_receive_processing a
WHERE
	a.mobile = b.mobile
;