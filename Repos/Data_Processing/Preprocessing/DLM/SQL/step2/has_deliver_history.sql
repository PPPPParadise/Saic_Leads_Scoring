set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
-- 是否荣威/名爵车主
DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_has_deliver_history;
CREATE TABLE marketing_modeling.dlm_tmp_has_deliver_history as  
SELECT
    a.mobile,
	1 as has_deliver_history
FROM
	(select mobile from marketing_modeling.dlm_tmp_vehicle_cleansing group by mobile) a,
	(select mobile from marketing_modeling.dlm_tmp_cust_cleansing group by mobile) b
where 
	a.mobile = b.mobile
;