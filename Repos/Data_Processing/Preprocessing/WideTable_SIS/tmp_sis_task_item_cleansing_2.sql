set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};
-- set hive.mapjoin.smalltable.filesize=55000000;
-- set hive.auto.convert.join=false;  -- #取消小表加载至内存中
--保留一位小数，根据ob_result_code判断通话结果
--（002转培育，003无人接听，004无法接通，005关机，006停机，007下发，008拒绝，009错误号码，010空号，011已接通），
--转培育/拒绝/已接通/下发为接通状态，取首次留资时间至最后一次建卡时间之间的外呼记录

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_task_item_cleansing_2;
CREATE TABLE marketing_modeling.tmp_dlm_task_item_cleansing_2 as 
SELECT 
	a.task_id,
    a.cust_id,
    a.mobile,
    a.task_item_id,
    a.ob_result_code,
	case
		when a.ob_result_code = '002' then 1
		when a.ob_result_code = '007' then 1
		when a.ob_result_code = '008' then 1
		when a.ob_result_code = '011' then 1
		else 0
	end as is_connect_status,
	case 
		when a.task_type in ('003','004') then 1
		else 0
	end as failed_call_client,
    a.ob_count,
    a.call_id,
    a.connid,
    a.created_time,
	b.talk_length,
	b.create_time,
	b.is_connected,
	ROW_NUMBER() OVER(PARTITION BY a.mobile,a.call_id ORDER BY a.created_time) AS rn
FROM
	marketing_modeling.tmp_dlm_task_item_cleansing_1 a,
	(select id,type,talk_length,create_time,is_connected 
	   from dtwarehouse.ods_smcsc_voip_call_record
	  where pt='${pt}' and create_time >= '${beginTime}') b
WHERE a.call_id = b.id
  and b.type = 2
;