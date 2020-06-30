set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_cust_processing;
CREATE TABLE marketing_modeling.tmp_dlm_cust_processing as  
SELECT
    a.mobile,
	a.cust_type,             --客户属性 (个人或企业)
	a.first_time as fir_card_time,    -- 首次建卡时间
	b.second_time,           -- 二次建卡时间
	a.last_time,             --最后建卡时间
--	a.deal_flag,             -- 是否成交
	c.total as card_ttl,     -- 总建卡次数
	c.cust_ids,
	c.last_followup_time,    --最后跟进时间
	c.clue_issued_times        -- 线索总下发次数     
FROM
	(
		SELECT
			mobile,
			create_time as first_time ,
			last_time,
			deal_flag,
			cust_type,
			age
		FROM
			marketing_modeling.tmp_dlm_cust_cleansing 
		WHERE rn = 1
	) a 
	left join 
	(
		SELECT
			mobile,
			create_time as second_time   	
		FROM 
			marketing_modeling.tmp_dlm_cust_cleansing 
		WHERE rn = 2 
	) b on a.mobile = b.mobile 
	left join 
	(
		SELECT 
			mobile,
			count(distinct cust_id) as total,   
			collect_set(cust_id) as cust_ids,
			sum(followup_amount) as clue_issued_times,
			max(last_followup_time) as last_followup_time
		FROM 
			marketing_modeling.tmp_dlm_cust_cleansing 
		 group by mobile
	 ) c on a.mobile = c.mobile 
	 ;