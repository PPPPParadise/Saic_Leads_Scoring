set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_finance_click_tb;
create table marketing_modeling.gio_tmp_finance_click_tb as 
--点击过金融服务;

SELECT device_id, 1 AS finance_click
FROM rdtwarehouse.cdm_growingio_action_hma
WHERE ( platform = 'Web'
        AND act_name LIKE '%金融计算器%')
    OR (act_name LIKE '%金融服务%'
        AND (platform = 'iOS'
        OR platform = 'Android'))
GROUP BY device_id;