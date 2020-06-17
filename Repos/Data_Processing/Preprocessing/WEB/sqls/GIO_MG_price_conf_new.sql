set mapreduce.map.memory.mb=4096; 
set mapreduce.reduce.memory.mb=8192; 
 
drop table if exists marketing_modeling.gio_tmp_price_config_tb; 
create table marketing_modeling.gio_tmp_price_config_tb as  
-- 过去7天点击名爵车系页-价格与配置页总次数
 
SELECT  
    device_id,  
    count(*) as price_config, 
    7 as type  
FROM  
	rdtwarehouse.cdm_growingio_action_hma  
where  
	act_name like '上汽名爵%' and 
	act_name like '%价格与配置%' and 
	datediff(current_timestamp(), ts) <= 7 
group by device_id 
union all 
	 
-- 过去15天点击名爵车系页-价格与配置页总次数
 
SELECT  
    device_id,  
    count(*) as price_config, 
    15 as type  
FROM  
	rdtwarehouse.cdm_growingio_action_hma  
where  
	act_name like '上汽名爵%' and 
	act_name like '%价格与配置%' and 
	datediff(current_timestamp(), ts) <= 15 
group by device_id 
union all 
-- 过去30天点击名爵车系页-价格与配置页总次数
 
 
SELECT  
    device_id,  
    count(*) as price_config, 
    30 as type  
FROM  
	rdtwarehouse.cdm_growingio_action_hma  
where  
	act_name like '上汽名爵%' and 
	act_name like '%价格与配置%' and 
	datediff(current_timestamp(), ts) <= 30 
group by device_id 
union all 
-- 过去45天点击名爵车系页-价格与配置页总次数
 
 
SELECT  
    device_id,  
    count(*) as price_config, 
    45 as type  
FROM  
	rdtwarehouse.cdm_growingio_action_hma  
where  
	act_name like '上汽名爵%' and 
	act_name like '%价格与配置%' and 
	datediff(current_timestamp(), ts) <= 45 
group by device_id;