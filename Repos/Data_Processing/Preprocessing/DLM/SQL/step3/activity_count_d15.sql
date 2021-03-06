set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_activity_count_d15;

CREATE TABLE marketing_modeling.dlm_tmp_activity_count_d15 as 
SELECT 
	a1.mobile,
	a1.activity_count_d15
FROM 
(
	SELECT
		a.mobile, 
		count(*) as activity_count_d15   --过去15天参加活动次数
	FROM
		marketing_modeling.dlm_tmp_cust_activity_cleansing a,
		marketing_modeling.dlm_tmp_last_deal_time b 
	WHERE
		a.mobile = b.mobile 
		and a.attend_time between date_sub(b.last_time,15) and b.last_time    --只计算战胜或最后一次战败的时间
	group by a.mobile
) a1 
;	
