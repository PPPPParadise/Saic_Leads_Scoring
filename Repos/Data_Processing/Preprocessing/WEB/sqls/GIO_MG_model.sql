
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists gio_tmp_mg_model_tb;
create table gio_tmp_mg_model_tb as 
SELECT 
    device_id,
	7 as type
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 7
UNION ALL
SELECT 
    device_id,
	15 as type
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 15
UNION ALL	
SELECT 
    device_id,
	30 as type
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 30
UNION ALL	
SELECT 
    device_id,
	45 as type
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 45


