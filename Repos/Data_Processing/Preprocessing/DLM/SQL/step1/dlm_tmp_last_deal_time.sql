---- 计算用户成交或战败的最后时间

DROP TABLE IF EXISTS marketing_modeling.dlm_tmp_last_deal_time;
CREATE TABLE marketing_modeling.dlm_tmp_last_deal_time as 
select 
	mobile,
	case 
		when deal_flag = 1 then last_deal_time 
		when deal_flag = 0 then last_dealfail_d
	end as last_time
from 
	marketing_modeling.dlm_tmp_feature_join_1
where deal_flag is not null
  
