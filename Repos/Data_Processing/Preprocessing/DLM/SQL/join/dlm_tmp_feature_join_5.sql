set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
--
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_feature_join_5;
CREATE TABLE marketing_modeling.dlm_tmp_feature_join_5 as 
SELECT 
	a1.mobile,
FROM
	marketing_modeling.dlm_tmp_mobile_mapping a1
left join 
	marketing_modeling.dlm_tmp_followup_d7 a2 on a1.mobile = a2.mobile 
;