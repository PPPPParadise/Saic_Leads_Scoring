
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists gio_tmp_app_web_browsing_tb;
create table gio_tmp_app_web_browsing_tb as 
--7天内在App/官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'all' as type,
	0 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 7
group by device_id
order by nums desc

UNION ALL
-- 3天内在App浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'app' as type,
	3 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 3 and
	applicationname in ('RWAPP', 'MGAPP')
group by device_id
order by nums desc

UNION ALL
-- 7天内在App浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'app' as type,
	7 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 7 and
	applicationname in ('RWAPP', 'MGAPP')
group by device_id
order by nums desc


UNION ALL
-- 3天内在官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'web' as type,
	3 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 3 and
	applicationname in ('RW官网', 'MG官网')
group by device_id
order by nums desc

UNION ALL
-- 7天内在官网浏览时间大于5分钟的次数
select 
	device_id,
	count(*) as nums,
	'web' as type,
	7 as period
from
	rdtwarehouse.cdm_growingio_activity_hma
where
	duration >=300 and
	datediff(current_timestamp(), ts) <= 7 and
	applicationname in ('RW官网', 'MG官网')
group by device_id
order by nums desc
