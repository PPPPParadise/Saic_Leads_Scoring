---- 计算用户成交或战败的最后时间
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_last_deal_time;
CREATE TABLE marketing_modeling.tmp_dlm_last_deal_time as 
select 
	mobile,
	case 
		when deal_flag = 1 then last_deal_time 
		when deal_flag = 0 then last_dealfail_d
	end as last_time
from 
	marketing_modeling.tmp_dlm_feature_join_1
where deal_flag is not null
  
