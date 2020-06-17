set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb;
create table marketing_modeling.gio_tmp_ttl_browse_time_avg45_tb as 
--过去45天内平均浏览时长
SELECT 
    p.device_id as device_id,
	CASE WHEN a.dur_15 is null THEN 0 ELSE a.dur_15 END as dur_15,
	CASE WHEN b.dur_45 is null THEN 0 ELSE b.dur_45 END as dur_45,
	CASE WHEN c.dur_30 is null THEN 0 ELSE c.dur_30 END as dur_30,
    (dur_15+dur_45+dur_30)/3 as dur_avg
from
    (select distinct(device_id) from rdtwarehouse.cdm_growingio_activity_hma) p
    left join
    marketing_modeling.gio_tmp_ttl_browse_time_d15_tb a
    on p.device_id = a.device_id
    left join
    marketing_modeling.gio_tmp_ttl_browse_time_d45_tb b
	ON a.device_id = b.device_id
	left join
    marketing_modeling.gio_tmp_ttl_browse_time_d30_tb c
    ON b.device_id = c.device_id
WHERE
    p.device_id is not null;