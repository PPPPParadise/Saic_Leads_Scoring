
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_app_web_browsing_tb;
create table marketing_modeling.gio_tmp_app_web_browsing_tb as 
--7天内在App/官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'all' as type,
	7 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 7
group by device_id

UNION ALL
-- 3天内App/官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'all' as type,
	3 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 3
group by device_id

union all
-- 15天内App/官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'all' as type,
	15 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 15
group by device_id
