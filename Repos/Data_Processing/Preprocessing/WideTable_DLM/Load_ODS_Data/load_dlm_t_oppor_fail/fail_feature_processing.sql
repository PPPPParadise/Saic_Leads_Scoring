set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

--失败数据统计
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_fail_processing;
CREATE TABLE marketing_modeling.tmp_dlm_fail_processing as  
SELECT
    a.mobile,
	a.first_time as fir_dealfail_d,    -- 首次战败时间
	c.second_time,   -- 倒数第二次战败时间
	a.last_time,    --最后战败时间   
	b.fail_cust_count,   -- 卡号战败次数
	b.total as deal_fail_times  --战败总次数
FROM
	(
		SELECT
			mobile,
			first_time, 
			create_time as last_time
		FROM
			marketing_modeling.tmp_dlm_fail_cleansing2 
		where rn2 = 1 
	) a 
	left join 
	(
		SELECT 
			mobile,
			count(*) as total,
			count(distinct cust_id) as fail_cust_count
		FROM 
			marketing_modeling.tmp_dlm_fail_cleansing2 
		 group by mobile
	 ) b on a.mobile = b.mobile 
	 left join 
	(
		SELECT 
			mobile,
			create_time as second_time
		FROM 
			marketing_modeling.tmp_dlm_fail_cleansing2 
		WHERE rn2 = 2
	 ) c on a.mobile = c.mobile 
	 ;