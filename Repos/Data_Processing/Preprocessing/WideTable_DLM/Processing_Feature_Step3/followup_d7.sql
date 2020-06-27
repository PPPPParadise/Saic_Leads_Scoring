set mapreduce.job.queuename=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_followup_d7;

CREATE TABLE marketing_modeling.tmp_dlm_followup_d7 as 
SELECT 
	a1.mobile,
	a1.followup_d7
FROM 
(
	SELECT
		a.mobile, 
		count(*) as followup_d7   --过去7天跟进次数
	FROM
		marketing_modeling.tmp_dlm_followup_cleansing a,
		marketing_modeling.tmp_dlm_last_deal_time b 
	WHERE
		a.mobile = b.mobile 
		and a.follow_time between date_sub(b.last_time,7) and b.last_time    --只计算战胜或最后一次战败的时间
	group by a.mobile
) a1
