set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

-- 成交时间差计算
DROP TABLE IF EXISTS marketing_modeling.tmp_leads_pool_status;
CREATE TABLE marketing_modeling.tmp_leads_pool_status AS 
---未下发
WITH t1 AS (
	SELECT a.mobile,'001' AS status
	FROM
		marketing_modeling.tmp_dlm_leads_processing a
--		,marketing_modeling.tmp_dlm_task_item_cleansing_2 b
	WHERE a.last_time >= date_sub(current_date(),180)
	 AND a.mobile NOT IN (SELECT mobile FROM marketing_modeling.tmp_dlm_cust_processing)
--	 AND a.mobile = b.mobile
--	 AND b.ob_result_code in ('002','003','004')
 ),
t2 AS (
	----培育 （战败 or （未成交 and 一个月未跟进）) 
	SELECT a.mobile,'002' AS status
	FROM
		marketing_modeling.tmp_dlm_leads_processing a,
		marketing_modeling.tmp_dlm_deal_flag_feature b 
	WHERE 
	  a.last_time >= date_sub(current_date(),180)
	  AND a.mobile = b.mobile
	  AND b.deal_flag = 0
	UNION
	SELECT a.mobile,'002' AS status
	FROM
		marketing_modeling.tmp_dlm_leads_processing a,
		marketing_modeling.tmp_dlm_deal_flag_feature b,
		marketing_modeling.tmp_dlm_followup_processing c 
	WHERE a.last_time >= date_sub(current_date(),180)
	  AND a.mobile = b.mobile
	  AND b.deal_flag = -1 
	  AND b.mobile =c.mobile 
	  AND c.last_time >= date_sub(current_date(),30)
),
t3 AS 
(
	SELECT 
		b.mobile,'003' AS status
	FROM 
		marketing_modeling.tmp_dlm_deal_flag_feature b
	WHERE 
		b.deal_flag = -1 and b.mobile not in (select mobile from t2)
)
SELECT 
    coalesce(t1.mobile,t2.mobile,t3.mobile) as mobile,
    coalesce(t1.status,t2.status,t3.status) as leads_pool_status
FROM T1 
	FULL JOIN T2 ON t1.mobile = t2.mobile
    FULL JOIN T3 ON t1.mobile = t3.mobile
;
