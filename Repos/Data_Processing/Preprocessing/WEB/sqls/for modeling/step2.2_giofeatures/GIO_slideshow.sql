set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_slideshow_tb;
create table marketing_modeling.gio_tmp_slideshow_tb as 
--爱车-轮播图点击;

SELECT device_id, 1 AS lovecar_slideshow
FROM marketing_modeling.gio_action_map
WHERE 
    act_name LIKE '%爱车%'
    AND act_name LIKE '%轮播%'
GROUP BY device_id;