set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
drop table if exists marketing_modeling.gio_action_map;
create table marketing_modeling.gio_action_map as 
--将mobile和flag, 以及成交战败时间map到action表
select 
    a.mobile as mobile,
    a.deal_flag as deal_flag,
    a.status_date as status_date,
	a.first_lead_time,
    b.*
from 
    (select * from marketing_modeling.deal_flag_date_map) a
left join
    rdtwarehouse.cdm_growingio_action_hma b
on a.device_id = b.device_id
where ts < status_date and
	  ts >= first_lead_time;
