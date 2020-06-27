set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_trail_count_d30;

CREATE TABLE marketing_modeling.tmp_dlm_trail_count_d30 as 
SELECT 
	a1.mobile,
	a1.trail_count_d30
FROM 
(
	SELECT
		a.mobile, 
		count(*) as trail_count_d30   --过去30天总试驾数
	FROM
		marketing_modeling.tmp_dlm_trial_receive_cleansing a,
		marketing_modeling.tmp_dlm_last_deal_time b 
	WHERE
		a.mobile = b.mobile 
		and status in ('14021003','14021004','14021005')
		and a.trial_time between date_sub(b.last_time,30) and b.last_time    --只计算战胜或最后一次战败的时间
	group by a.mobile
) a1
;	
