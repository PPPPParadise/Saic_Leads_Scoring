set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_ttl_browse_time_diff_tb;
create table marketing_modeling.gio_tmp_ttl_browse_time_diff_tb as 
--过去45天内窗口一浏览时长与窗口三浏览时长差值
SELECT 
    p.device_id as device_id,
	CASE WHEN a.dur_15 is not null and b.dur_45 is not null THEN (a.dur_15 - b.dur_45)
		 WHEN a.dur_15 is null and b.dur_45 is not null THEN (0-b.dur_45)
		 WHEN a.dur_15 is not null and b.dur_45 is not null THEN a.dur_15
		 ELSE 0
		end as dur_diff
FROM 
    (SELECT DISTINCT(device_id) FROM marketing_modeling.gio_activity_map) p
    left join
    marketing_modeling.gio_tmp_ttl_browse_time_d15_tb a
    on p.device_id = a.device_id
    left join
    marketing_modeling.gio_tmp_ttl_browse_time_d45_tb b
    ON a.device_id = b.device_id;
    
    