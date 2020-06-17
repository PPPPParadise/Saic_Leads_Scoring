DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_followup_d15;

CREATE TABLE marketing_modeling.dlm_tmp_followup_d15 as 
SELECT 
	a1.mobile,
	a1.followup_d15
FROM 
(
	SELECT
		a.mobile, 
		count(*) as followup_d15   --过去7天跟进次数
	FROM
		marketing_modeling.dlm_tmp_followup_cleansing a,
		marketing_modeling.dlm_tmp_last_deal_time b 
	WHERE
		a.mobile = b.mobile 
		and a.follow_time between date_sub(b.last_time,15) and b.last_time    --只计算战胜或最后一次战败的时间
	group by a.mobile
) a1
