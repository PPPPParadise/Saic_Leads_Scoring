
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_ttl_browse_time_d15_tb;
create table marketing_modeling.gio_tmp_ttl_browse_time_d15_tb as 
--获取15天的总浏览时常
select 
    device_id, 
    sum(duration) as dur_15
from 
	rdtwarehouse.cdm_growingio_activity_hma
where 
	datediff(current_timestamp(), ts) <=15 and
	applicationname LIKE '%MG%'
GROUP BY device_id;