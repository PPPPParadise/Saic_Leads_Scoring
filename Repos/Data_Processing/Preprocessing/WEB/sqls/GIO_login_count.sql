set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists gio_tmp_login_ttl;
create table gio_tmp_login_ttl as 
SELECT 
   device_id,
   count(*) as login_ttl
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '%登录%' and 
	act_name like '%按钮'
GROUP BY act_name