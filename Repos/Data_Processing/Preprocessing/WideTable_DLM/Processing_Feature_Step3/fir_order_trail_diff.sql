set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

--首次下定时间与首次到店时间的日期差值
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fir_order_visit_diff;
CREATE TABLE marketing_modeling.tmp_dlm_fir_order_visit_diff as 
SELECT
	a.mobile, 
	datediff(b.deposit_order_time,a.fir_visit_time ) as fir_order_visit_diff	 
FROM
	marketing_modeling.tmp_dlm_oppor_processing2 b,
	marketing_modeling.tmp_dlm_hall_flow_processing a
WHERE
	a.mobile = b.mobile
;