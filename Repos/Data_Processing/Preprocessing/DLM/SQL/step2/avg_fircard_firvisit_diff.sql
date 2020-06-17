set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_avg_fircard_firvisit_diff;
CREATE TABLE marketing_modeling.dlm_tmp_avg_fircard_firvisit_diff as  
SELECT
	a1.mobile,
	-- 客户在每个经销商下首次到店时间与建卡时间的日期差的平均值，若该客户在某个经销商下未到店，则不考虑该经销商
	collect_list(a1.fir_sec_diff) as fircard_firvisit_dtbt_diff,  --建卡时间距首次到店时间日期差值(每个经销商有个value)
	round(sum(a1.fir_sec_diff)/count(dealer_id),2) as avg_fircard_firvisit_diff --平均建卡时间距首次到店时间日期差值
FROM
(
	SELECT
		a.mobile,
		a.dealer_id,
		datediff(a.first_time,b.first_time) as fir_sec_diff
	FROM
	(
		SELECT
			mobile,
			dealer_id,
			arrive_time as first_time   
		FROM
			marketing_modeling.dlm_tmp_hall_flow_cleansing2  --到店
		WHERE rn2 = 1 and dealer_id is not null 
	) a 
	left join 
	(
		SELECT
			mobile,
			dealer_id,
			min(create_time) as first_time   	
		FROM 
			marketing_modeling.dlm_tmp_cust_cleansing  -- 建卡
		group by mobile,dealer_id 
	) b on (a.mobile = b.mobile and a.dealer_id = b.dealer_id)
) a1 
where a1.dealer_id is not null 
group by a1.mobile
;
	 