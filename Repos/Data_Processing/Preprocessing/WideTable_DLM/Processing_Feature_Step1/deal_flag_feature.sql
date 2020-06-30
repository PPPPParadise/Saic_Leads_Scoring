set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};
--标记状态，成交/战败/跟进中
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_deal_flag_processing;
CREATE TABLE marketing_modeling.tmp_dlm_deal_flag_processing as  
with t1 as (
    select 
		cust_id,
		dealer_id 
    from 
		marketing_modeling.tmp_dlm_fail_cleansing 
    group by cust_id,dealer_id
),
t2 as (
    select 
    	a.cust_id,
    	a.mobile,
    	a.dealer_id,
    	a.create_time as cust_time,
    	if(a.deal_flag ='N',if(b.dealer_id is null,NULL,'N'),a.deal_flag ) as deal_flag
    from 
    	marketing_modeling.tmp_dlm_cust_cleansing a
        left join t1 b on (a.cust_id = b.cust_id and a.dealer_id = b.dealer_id)
) 
select 
    t2.mobile,
    sum(case when t2.deal_flag = 'N' then 1 else 0 end) as fail_num,  -- 战败数
    sum(case when t2.deal_flag = 'Y' then 1 else 0 end) as deal_num,  -- 战胜数
    sum(case when t2.deal_flag is null then 1 else 0 end) as followup_num,  --跟进中
    count(t2.dealer_id) as dealer_num                                 -- 总经销商数
from t2
group by t2.mobile
;

-- 建卡用户计算完成后，就需要计算这个，用来筛选成交、战败人群，也可以过滤掉跟进中的人群
DROP TABLE IF EXISTS marketing_modeling.tmp_dlm_deal_flag_feature;
CREATE TABLE marketing_modeling.tmp_dlm_deal_flag_feature as 
select mobile,
	case 
		when fail_num = dealer_num then 0
		when deal_num > 0 then 1
		else -1
	end as deal_flag	
from marketing_modeling.tmp_dlm_deal_flag_processing
; 