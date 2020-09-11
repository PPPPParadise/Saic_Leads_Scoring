set tez.queue.name=${queuename};
drop table if exists marketing_modeling.tmp_ttl_browse_time;
create table marketing_modeling.tmp_ttl_browse_time as 
	select 
		b.phone, 
		sum(a.duration) as dur
	from 
		(select * from dtwarehouse.cdm_growingio_activity_hma where 
		datediff(current_timestamp(), ts) =14 and
		applicationname LIKE '%MG%') a
	join  cdp.edw_cdp_id_mapping_overview b
	on b.pt='${pt}' and a.device_id = b.device_id 
	where b.phone regexp "^[1][3-9][0-9]{9}$" 
	GROUP BY b.phone
	