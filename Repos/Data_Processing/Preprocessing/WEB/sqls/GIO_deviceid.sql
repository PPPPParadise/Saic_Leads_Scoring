set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_deviceid;
create table marketing_modeling.gio_tmp_deviceid as 
--获取所有device_id
select device_id
from rdtwarehouse.cdm_growingio_action_hma 
where
	device_id is not null and
	device_id != ''
GROUP BY device_id
union all
select device_id
from rdtwarehouse.cdm_growingio_activity_hma 
where
	device_id is not null and
	device_id != '';