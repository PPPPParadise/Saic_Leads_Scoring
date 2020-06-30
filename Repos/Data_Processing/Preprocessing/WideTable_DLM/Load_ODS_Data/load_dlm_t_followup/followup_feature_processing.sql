set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_followup_processing;
CREATE TABLE marketing_modeling.tmp_dlm_followup_processing as  
SELECT
    a.mobile,
	a.first_time,    -- 首次跟进时间
	b.second_time,           -- 二次跟进时间
	c.last_time,             --最后跟进时间
	c.follow_total as followup_ttl     -- 总跟进次数
FROM
	(
		SELECT
			mobile,
			follow_time as first_time
		FROM
			marketing_modeling.tmp_dlm_followup_cleansing2 
		WHERE rn2 = 1
	) a 
	left join 
	(
		SELECT
			mobile,
			follow_time as second_time   	
		FROM 
			marketing_modeling.tmp_dlm_followup_cleansing2 
		WHERE rn2 = 2 
	) b on a.mobile = b.mobile 
	left join 
	(
		SELECT 
			mobile,
			count(*) as follow_total ,
			max(follow_time) as last_time
		FROM 
			marketing_modeling.tmp_dlm_followup_cleansing2 
		 group by mobile
	 ) c on a.mobile = c.mobile 
	 ;