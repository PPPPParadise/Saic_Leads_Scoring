set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_app_script_step2;
CREATE TABLE marketing_modeling.tmp_app_script_step2 AS
SELECT
	
	mobile,
	-1 as dealer_id,
	case when funcs = 1 then '21,22'
		when funcs = 2 then '9,11,22'
		when funcs = 3 then '2,3,4,5,6,13,14,15,19,20'
		when funcs = 4 then '2,3,4,5,6,13,14,15,19,20'
		when funcs = 6 then '7,8,12,16,17,18'
		when funcs = 7 then '9'
		end as auto_script_id,
	current_timestamp() as create_time,
	1 as orders
FROM marketing_modeling.tmp_funcs_processing