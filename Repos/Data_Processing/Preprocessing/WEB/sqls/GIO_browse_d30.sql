
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_ttl_browse_time_d30_tb;
create table marketing_modeling.gio_tmp_ttl_browse_time_d30_tb as 
--获取30天的总浏览时常
select 
    device_id, 
    sum(duration) as dur_30
from 
	rdtwarehouse.cdm_growingio_activity_hma
where 
	datediff(current_timestamp(), ts) >15 and
	datediff(current_timestamp(), ts) <=30 and
	applicationname LIKE '%MG%'
GROUP BY device_id;