set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_receive_processing;
CREATE TABLE marketing_modeling.dlm_tmp_receive_processing as  
SELECT
    a.mobile,
	b.first_time as fir_trail,    --- 首次试乘试驾时间
	a.last_time,
	a.last_reservation_time,      -- 最后一次试驾预约试驾
	b.trial_total as trail_book_tll,  ------ 试乘试驾总预定次数
	c.trial_total as trail_attend_ttl,  ---/试乘试驾总参与次数
	round(c.trial_total/b.trial_total,2) as trail_attend_ppt --/试乘试驾参与率   
	
FROM
	(
		SELECT
			mobile,
			trial_time as last_time,
			last_reservation_time
		FROM
			marketing_modeling.dlm_tmp_trial_receive_cleansing2 
		WHERE rn2 = 1
	) a 
	left join 
	(
		SELECT 
			mobile,
			count(*) as trial_total,
			MIN(trial_time) as first_time
		FROM 
			marketing_modeling.dlm_tmp_trial_receive_cleansing2  
		group by mobile
	 ) b on a.mobile = b.mobile 
	left join 
	(
		SELECT 
			mobile,
			count(*) as trial_total 
		FROM 
			marketing_modeling.dlm_tmp_trial_receive_cleansing2 
		WHERE
			status in ('14021003','14021004','14021005')
		group by mobile
	 ) c on a.mobile = c.mobile  
	 ;
-- 过去30天试乘试驾次数
-- 过去90天试乘试驾次数