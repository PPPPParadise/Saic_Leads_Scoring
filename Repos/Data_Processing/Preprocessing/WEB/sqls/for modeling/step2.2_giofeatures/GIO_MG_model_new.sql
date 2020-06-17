set mapreduce.map.memory.mb=4096; 
set mapreduce.reduce.memory.mb=8192; 
 
drop table if exists marketing_modeling.gio_tmp_mg_model_tb; 
create table marketing_modeling.gio_tmp_mg_model_tb as  
SELECT  
    device_id, 
    count(*) as model, 
	7 as type 
FROM  
	marketing_modeling.gio_action_map 
where  
	act_name like '上汽名爵%' and 
	act_name like '%车系页%' and
	datediff(first_lead_time, ts) <= 7 
group by device_id 
UNION ALL 
SELECT  
    device_id, 
    count(*) as model, 
	15 as type 
FROM  
	marketing_modeling.gio_action_map
where  
	act_name like '上汽名爵%' and 
	act_name like '%车系页%' and 
	datediff(first_lead_time, ts) <= 15 
group by device_id 
UNION ALL	 
SELECT  
    device_id, 
    count(*) as model, 
	30 as type 
FROM  
	marketing_modeling.gio_action_map  
where  
	act_name like '上汽名爵%' and 
	act_name like '%车系页%' and 
	datediff(first_lead_time, ts) <= 30 
group by device_id 
UNION ALL	 
SELECT  
    device_id, 
    count(*) as model, 
	45 as type 
FROM  
	marketing_modeling.gio_action_map 
where  
	act_name like '上汽名爵%' and 
	act_name like '%车系页%' and 
	datediff(first_lead_time, ts) <= 45 
group by device_id; 