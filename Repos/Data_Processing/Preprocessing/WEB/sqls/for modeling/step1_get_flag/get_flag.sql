set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.deal_flag_date;
create table marketing_modeling.deal_flag_date as 
--从DLM小宽表提取mobile和flag
SELECT 
    a.mobile,
    a.deal_flag,
    CASE WHEN a.deal_flag = 'Y' THEN a.deal_time ELSE a.last_dealfail_d end as status_date,
	b.create_time as first_lead_time --首次留资时间
FROM 
    marketing_modeling.mm_dlm_behavior_wide_info a
left join
	marketing_modeling.dlm_tmp_leads_cleansing b
on 
	a.mobile = b.mobile and b.rn = 1
	
WHERE a.deal_flag is NOT NULL;