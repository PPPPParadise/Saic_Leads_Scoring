set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_ttl_browse_time_d45_tb;
create table marketing_modeling.gio_tmp_ttl_browse_time_d45_tb as 
--获取45天的总浏览时常
select 
    device_id, 
    sum(duration) as dur_45
from 
	marketing_modeling.gio_activity_map
where 
	datediff(first_lead_time, ts) <=45 and
	datediff(first_lead_time, ts) >30 and 
	applicationname LIKE '%MG%'
GROUP BY device_id;