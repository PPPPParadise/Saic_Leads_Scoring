set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_article_tb;
create table marketing_modeling.gio_tmp_article_tb as 
-- 小于15天的文章点击次数
SELECT 
    device_id,
    15 as type,
    count(*) as ttl
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '%文章%' and
	datediff(current_timestamp(), ts) <= 15 and
	applicationname LIKE '%MG%'
GROUP BY
    device_id
UNION ALL
-- 小于7天的文章点击次数
SELECT 
    device_id,
    7 as type,
    count(*) as ttl
FROM 
	rdtwarehouse.cdm_growingio_action_hma 
where 
	act_name like '%文章%' and
	datediff(current_timestamp(), ts) <= 7 and
	applicationname LIKE '%MG%'
GROUP BY
    device_id
