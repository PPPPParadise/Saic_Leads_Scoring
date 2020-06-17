set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_find_dealers_tb;
create table marketing_modeling.gio_tmp_find_dealers_tb as 
--Web点击购车工具-查找经销商，App点击爱车-全部4S店按钮
-- 目前只有app有埋点
SELECT 
    device_id,
    1 as result
FROM 
	marketing_modeling.gio_action_map
where 

	act_name like '%全部4S店按钮' or
	act_name like '%查找经销商'
group by device_id;