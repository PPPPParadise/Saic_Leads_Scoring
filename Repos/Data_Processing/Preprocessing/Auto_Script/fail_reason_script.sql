set tez.queue.name=${queuename};
DROP TABLE IF EXISTS marketing_modeling.tmp_fail_reason_script;
CREATE TABLE IF NOT EXISTS marketing_modeling.tmp_fail_reason_script AS
SELECT
	a.mobile,
	CASE WHEN a.fail_reason like '%竞品意向客户%' then 29
		WHEN a.fail_reason like '%无购车需求%' then 30
		WHEN a.fail_reason like '%暂不买车%' then 31
		WHEN a.fail_reason like '%资金问题%' then 32
		WHEN a.fail_reason like '%家人/朋友反对%' then 33
		WHEN a.fail_reason like '%无购车资质%' then 34
		WHEN a.fail_reason like '%经销商问题%' then 35
	END AS auto_script_id
FROM
(

	SELECT
		mobile,
		fail_reason
	FROM
		marketing_modeling.app_failed_reason
	lateral view explode(fail_reason_fir_level) snTable as fail_reason   

    WHERE fail_reason != ''
) a