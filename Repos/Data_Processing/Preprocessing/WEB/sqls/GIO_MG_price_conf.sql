
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists gio_tmp_price_config_tb;
create table gio_tmp_price_config_tb as 
-- 过去7天点击名爵车系页-价格与配置页总次数
SELECT 
    device_id, 
    7 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%价格与配置%' and
	datediff(current_timestamp(), ts) <= 7;
	
	
-- 过去15天点击名爵车系页-价格与配置页总次数
SELECT 
    device_id, 
    15 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%价格与配置%' and
	datediff(current_timestamp(), ts) <= 15;
	
-- 过去30天点击名爵车系页-价格与配置页总次数

SELECT 
    device_id, 
    30 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%价格与配置%' and
	datediff(current_timestamp(), ts) <= 30;
	
-- 过去45天点击名爵车系页-价格与配置页总次数

SELECT 
    device_id, 
    45 as type 
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%价格与配置%' and
	datediff(current_timestamp(), ts) <= 45;
	
	
	
	
	