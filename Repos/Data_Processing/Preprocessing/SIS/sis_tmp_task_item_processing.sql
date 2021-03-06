set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中

DROP TABLE IF EXISTS marketing_modeling.mm_sis_wide_info;
CREATE TABLE marketing_modeling.mm_sis_wide_info as 
with t1 as (
	SELECT 
		a.mobile,
		a.ob_result_code,
		a.created_time,
		ROW_NUMBER() OVER(PARTITION BY a.mobile ORDER BY a.created_time) AS rn
	FROM
		marketing_modeling.dlm_tmp_task_item_cleansing_2 a
),
t2 as (
	SELECT 
		mobile,
		count(*) total,
		sum(is_connect_status) as connet_sum,
		sum(talk_length) as talk_length_sum
	FROM
		marketing_modeling.dlm_tmp_task_item_cleansing_2 a,
		marketing_modeling.mm_dlm_behavior_wide_info b
	where a.mobile = b.mobile
	  and a.created_time > b.fir_leads_time 
	  and a.created_time <= b.last_card_time
	group by a.mobile
),
t3 as (
	SELECT 
		mobile,
		count(*) total
	FROM
		marketing_modeling.dlm_tmp_task_item_cleansing_2 a,
		marketing_modeling.mm_dlm_behavior_wide_info b
	where a.mobile = b.mobile
	  and a.created_time > b.fir_leads_time 
	  and a.created_time <= b.fir_card_time
	group by a.mobile
)
select t1.mobile,
	t1.ob_result_code as first_call_result,   --首次通话结果
	t1.created_time as first_call_time,       --首次通话时间
	t2.total as firstleads_lastcard_call_count,              --首次留资时间和最后一次建卡时间之间的总外呼次数
	ROUND(t2.connet_sum/t2.total,2) as talk_connet_ppt,      ----接通率
	ROUND(t2.talk_length_sum/1000/60/t2.total) as avg_talk_length,  --平均通话时长（分钟）
	t3.total as firstleads_firstcard_call_count              --首次留资时间而和首次建卡时间之间的总外呼次数
from t1 
	left join t2 on t1.mobile = t2.mobile
	left join t3 on t1.mobile = t3.mobile
;

