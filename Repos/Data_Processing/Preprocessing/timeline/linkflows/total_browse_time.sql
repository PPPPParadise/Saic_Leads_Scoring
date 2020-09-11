set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.tmp_userprofile_browse_time;
create table marketing_modeling.tmp_userprofile_browse_time as 
--获取7天的总浏览次数
select b.phone,
		a.ttl_d7
from (
select 
    device_id, 
    count(1) as ttl_d7
from 
	dtwarehouse.cdm_growingio_activity_hma
where 
	datediff(current_timestamp(), ts) <= 7 and
	applicationname LIKE '%MG%'
GROUP BY device_id
) a
left join 
	(select * from cdp.edw_cdp_id_mapping_overview where pt='${pt}') b
on a.device_id = b.device_id and b.phone is not null