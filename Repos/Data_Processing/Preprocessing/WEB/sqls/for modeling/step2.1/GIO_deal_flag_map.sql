set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
drop table if exists marketing_modeling.deal_flag_date_map;
create table marketing_modeling.deal_flag_date_map as 
--将device_id map 到DLM提取出来的mobile和flag
select 
    b.device_id as device_id,
    a.mobile as mobile,
    a.deal_flag as deal_flag,
    a.status_date as status_date,
	a.first_lead_time
from 
    marketing_modeling.deal_flag_date a
    left join
    (select phone, device_id 
    from rdtwarehouse.edw_cdp_id_mapping_overview
    where phone is not null and
          phone != '' and
          device_id is not null and
          device_id != ''
    group by phone, device_id) b
    on a.mobile = b.phone
where 
    device_id is not null and 
    device_id != '';