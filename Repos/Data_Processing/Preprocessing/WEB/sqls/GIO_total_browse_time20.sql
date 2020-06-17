set mapreduce.map.memory.mb=4096; 
set mapreduce.reduce.memory.mb=8192; 
 
drop table if exists marketing_modeling.gio_tmp_ttl_browse_20_d7_tb; 
create table marketing_modeling.gio_tmp_ttl_browse_20_d7_tb as  
--获取7天的总浏览时常  
 
select  
    device_id,
    sum(duration)
from  
	rdtwarehouse.cdm_growingio_activity_hma 
where  
	datediff('2020-05-13 00:00:00', ts) <= 7 and
	ts < '2020-05-13 00:00:00' and
	applicationname LIKE '%MG%'
GROUP BY device_id 
having sum(duration) > 1200;

