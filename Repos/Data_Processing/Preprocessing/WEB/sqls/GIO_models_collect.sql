set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists gio_tmp_models_collect_tb;
create table gio_tmp_models_collect_tb as
-- 浏览过的车系页
SELECT 
    device_id,
    concat_ws('|', collect_set(split(act_name, '-')[2])) as models
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '上汽名爵%' and
	act_name like '%车系页%'
GROUP BY
    device_id
	