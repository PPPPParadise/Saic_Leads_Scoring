set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_hall_flow_processing;
CREATE TABLE marketing_modeling.tmp_dlm_hall_flow_processing as  
SELECT
    a.mobile,
	a.first_time as fir_visit_time,      --	首次到店时间
	b.second_time,
	c.last_time,
	datediff(b.second_time,a.first_time) as fir_sec_visit_diff,   ---	首次到店时间与二次到店时间日期差值
	c.visit_dtbt_count,     --	到店总经销商数量
	c.visit_ttl,           --	总到店次数
	round(datediff(c.last_time,a.first_time)/c.visit_ttl,2) as avg_visit_date,         --平均到店时间间隔
	c.dealer_ids,      
	d.avg_visit_dtbt_count   --	平均每个经销商处到店次数  
FROM
	(
		SELECT
			mobile,
			arrive_time as first_time
		FROM
			marketing_modeling.tmp_dlm_hall_flow_cleansing 
		WHERE rn = 1
	) a 
	left join 
	(
		SELECT
			mobile,
			arrive_time as second_time   	
		FROM 
			marketing_modeling.tmp_dlm_hall_flow_cleansing 
		WHERE rn = 2 
	) b on a.mobile = b.mobile 
	left join 
	(
		SELECT 
			mobile,
			count(distinct dealer_id) as visit_dtbt_count,   
			collect_set(dealer_id) as dealer_ids,
			count(*) as visit_ttl,
			max(arrive_time) as last_time
		FROM 
			marketing_modeling.tmp_dlm_hall_flow_cleansing 
		 group by mobile
	 ) c on a.mobile = c.mobile 
	 left join 
	(
		SELECT 
			d1.mobile,
			round(sum(d1.dealer_visit_count)/count(d1.dealer_id),2) as avg_visit_dtbt_count
		FROM 
		(
			SELECT 
				mobile,
				dealer_id,
				count(*) as dealer_visit_count
			FROM 
				marketing_modeling.tmp_dlm_hall_flow_cleansing 
			 group by mobile,dealer_id) d1
		group by d1.mobile
	 ) d on a.mobile = d.mobile 
	 ;
	 