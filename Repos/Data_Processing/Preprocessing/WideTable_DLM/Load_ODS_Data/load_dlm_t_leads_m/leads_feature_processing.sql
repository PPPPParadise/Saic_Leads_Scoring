set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_leads_processing;
CREATE TABLE marketing_modeling.tmp_dlm_leads_processing as  
SELECT
    a.mobile,
	a.first_time as fir_leads_time,    -- 首次留资时间
	b.second_time,           -- 二次留资时间
	if(b.second_time is null,NULL,datediff(b.second_time,a.first_time)) as fir_sec_leads_diff, -- 首次留资时间与二次留资时间日期差值
	a.last_time,             -- 最后留资时间
	c.total as leads_count,  -- 总留资次数
	-- c.clue_issued_times,     -- 线索总下发次数
	c.channels as leads_channel,   --留资渠道
	round(datediff(a.last_time,a.first_time)/total,2) as avg_leads_date,      -- 平均留资时间间隔
	c.channel_count as leads_channel_count,  -- 留资渠道总数
	c.leads_dtbt_count,      -- 留资总经销商数
	round(c.leads_dtbt_count / c.leads_dtbt_total , 2) as leads_dtbt_coincide,  -- 留资经销商重合度
	d.dealer_level_1 as leads_dtbt_level_1,    -- 一级经销商留资数量
	d.dealer_level_2 as leads_dtbt_level_2,    -- 二级经销商留资数量
	c.dealer_ids 
FROM 
	(
		SELECT
			mobile,
			create_time as first_time, 
			last_time
		FROM
			marketing_modeling.tmp_dlm_leads_cleansing
		WHERE rn = 1
	) a 
	left join 
	(
		SELECT
			mobile,
			create_time as second_time   	
		FROM 
			marketing_modeling.tmp_dlm_leads_cleansing 
		WHERE rn = 2 
	) b on a.mobile = b.mobile 
	left join 
	(
		SELECT 
			mobile,
			count(*) as total,  
			collect_set(channel) as channels,
			count(distinct channel) as channel_count,         -- 留资渠道总数
			count(distinct dealer_id) as leads_dtbt_count,         -- 留资总经销商数
			count(dealer_id) as leads_dtbt_total,
			collect_set(dealer_id) as dealer_ids
			-- count(distinct cust_id) as clue_issued_times
		FROM 
			marketing_modeling.tmp_dlm_leads_cleansing 
		group by mobile
	 ) c on a.mobile = c.mobile 
	 left join 
	 (
		select 
			 mobile,
			sum( case when dealer_level='A' then 1 else 0 end ) as dealer_level_1,
			sum( case when dealer_level='B' then 1 else 0 end ) as dealer_level_2
		from (
			select mobile,dealer_id,dealer_level
			from 
				marketing_modeling.tmp_dlm_leads_cleansing
			group by mobile,dealer_id,dealer_level) a
		group by mobile
	) d on a.mobile = d.mobile 
   ;

