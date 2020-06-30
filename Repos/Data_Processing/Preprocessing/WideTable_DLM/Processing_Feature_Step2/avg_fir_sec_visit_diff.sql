set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_avg_fir_sec_visit_diff;
CREATE TABLE marketing_modeling.tmp_dlm_avg_fir_sec_visit_diff as  
SELECT
	a1.mobile,
	-- 客户在每个经销商下首次到店时间与二次到店时间日期差值的平均值，若该客户在该经销商下未二次到店，则不考虑该经销商
	round(sum(a1.fir_sec_diff)/count(dealer_id),2) as avg_fir_sec_visit_diff --平均首次到店时间与二次到店时间日期差值
FROM
(
	SELECT
		a.mobile,
		a.dealer_id,
		datediff(a.second_time,b.first_time) as fir_sec_diff
	FROM
	(
		SELECT
			mobile,
			dealer_id,
			arrive_time as second_time
		FROM
			marketing_modeling.tmp_dlm_hall_flow_cleansing2 
		WHERE rn2 = 2 and dealer_id is not null 
	) a 
	left join 
	(
		SELECT
			mobile,
			dealer_id,
			arrive_time as first_time   	
		FROM 
			marketing_modeling.tmp_dlm_hall_flow_cleansing2 
		WHERE rn2 = 1 
	) b on (a.mobile = b.mobile and a.dealer_id = b.dealer_id)
) a1 
where a1.dealer_id is not null
 group by a1.mobile
;
	 