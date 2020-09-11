set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_followup_auto_script;
CREATE TABLE marketing_modeling.tmp_followup_auto_script AS
WITH base AS(
SELECT  mobile,
		s_first_call_result,
		c_visit_count,
		d_trail_attend_ttl
		
FROM
	marketing_modeling.app_big_wide_info
WHERE
	pt='${pt}'
),
sis_count AS (
	SELECT 	mobile,
			count(*) as calling_count
	FROM
			marketing_modeling.tmp_dlm_task_item_cleansing_2
	GROUP BY
			mobile
)
SELECT
	a.*,
	b.calling_count
FROM
	base a
LEFT JOIN sis_count b
ON 	a.mobile = b.mobile
WHERE
	a.s_first_call_result is not null or
	a.c_visit_count is not null or
	a.d_trail_attend_ttl is not null or
	b.calling_count is not null 