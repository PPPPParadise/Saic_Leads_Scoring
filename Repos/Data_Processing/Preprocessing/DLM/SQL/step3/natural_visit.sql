set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

--自然到店  到店时间早于留资时间
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_natural_visit;
CREATE TABLE marketing_modeling.dlm_tmp_natural_visit as 
SELECT
	a.mobile, 
	'1' as natural_visit	 
FROM
	marketing_modeling.dlm_tmp_hall_flow_processing a,
	marketing_modeling.dlm_tmp_leads_processing b
WHERE
	a.mobile = b.mobile
	and b.fir_leads_time > a.fir_visit_time
;