set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_followup_d90;

CREATE TABLE marketing_modeling.tmp_dlm_followup_d90 as 
SELECT 
	a1.mobile,
	a1.followup_d90
FROM 
(
	SELECT
		a.mobile, 
		count(*) as followup_d90   --过去90天跟进次数
	FROM
		marketing_modeling.tmp_dlm_followup_cleansing a,
		marketing_modeling.tmp_dlm_last_deal_time b 
	WHERE
		a.mobile = b.mobile 
		and a.follow_time between date_sub(b.last_time,90) and b.last_time    --只计算战胜或最后一次战败的时间
	group by a.mobile
) a1
;	
