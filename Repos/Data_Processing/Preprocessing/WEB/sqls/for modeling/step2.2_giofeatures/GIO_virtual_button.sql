
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_virtual_tb;
create table marketing_modeling.gio_tmp_virtual_tb as 

SELECT 
    device_id,
    1 AS lovecar_virtual_botton
FROM 
	marketing_modeling.gio_action_map 
WHERE 
    act_name LIKE '%虚拟车控%'
GROUP BY device_id;