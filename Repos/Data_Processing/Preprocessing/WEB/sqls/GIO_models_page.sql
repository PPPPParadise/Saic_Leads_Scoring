set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_model_page_tb;
create table marketing_modeling.gio_tmp_model_page_tb as  
-- 过去7天点击车系页涉及车型页的总次数
select 
    device_id, 
    count(distinct((split(act_name, '-')[2]))) as model_page,
	7 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 7
GROUP BY
    device_id
	
union all	
	
-- 过去15天点击车系页涉及车型页的总次数
select 
    device_id, 
    count(distinct((split(act_name, '-')[2]))) as model_page,
	15 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 15
GROUP BY
    device_id
	
union all	
	
-- 过去30天点击车系页涉及车型页的总次数
select 
    device_id, 
    count(distinct((split(act_name, '-')[2]))) as model_page,
	30 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 30
GROUP BY
    device_id
	
union all	
	
-- 过去45天点击车系页涉及车型页的总次数
select 
    device_id, 
    count(distinct((split(act_name, '-')[2]))) as model_page,
	45 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%' and
	datediff(current_timestamp(), ts) <= 45
GROUP BY
    device_id;	