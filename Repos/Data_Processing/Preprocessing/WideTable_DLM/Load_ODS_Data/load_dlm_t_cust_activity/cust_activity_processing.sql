set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set hive.mapjoin.smalltable.filesize=55000000;
set hive.auto.convert.join=false;  -- #取消小表加载至内存中
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_cust_activity_processing;
CREATE TABLE marketing_modeling.tmp_dlm_cust_activity_processing as  
SELECT
    a.mobile,
	a.first_time as fir_activity_time,    -- 首次活动时间
	a.last_time,             --最后活动时间
	b.total as activity_ttl     -- 总活动次数    
FROM
	(
		SELECT
			mobile,
			first_time ,
			attend_time as last_time
		FROM
			marketing_modeling.tmp_dlm_cust_activity_cleansing2 
		WHERE rn = 1
	) a 
	left join 
	(
		SELECT 
			mobile,
			count(*) as total  ---参加活动总次数
		FROM 
			marketing_modeling.tmp_dlm_cust_activity_cleansing2 
		 group by mobile
	 ) b on a.mobile = b.mobile 
;