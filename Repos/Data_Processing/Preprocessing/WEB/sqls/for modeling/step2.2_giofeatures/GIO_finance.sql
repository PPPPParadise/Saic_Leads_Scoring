set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
drop table if exists marketing_modeling.gio_tmp_finance_tb;
create table marketing_modeling.gio_tmp_finance_tb as 
--过去7天点击金融服务次数;
SELECT 
    a.device_id as device_id,
    sum(a.finance) as finance,
    a.type as type
FROM 
(SELECT 
	device_id, 
	count(*) AS finance,
	7 AS type
FROM 
	marketing_modeling.gio_action_map
WHERE  
	( act_name LIKE '%金融服务%'
      AND (platform = 'iOS'
      OR platform = 'Android')
      AND datediff(first_lead_time,ts) <= 7)
GROUP BY device_id
union all
--过去15天点击金融服务次数;
SELECT 
	device_id, 
	count(*) AS finance,
	15 AS type
FROM 
	marketing_modeling.gio_action_map
WHERE  
	( act_name LIKE '%金融服务%'
      AND (platform = 'iOS'
      OR platform = 'Android')
      AND datediff(first_lead_time,ts) <= 15)
GROUP BY device_id
union all
--过去30天点击金融服务次数;

SELECT 
	device_id, 
	count(*) AS finance,
	30 AS type
FROM 
	marketing_modeling.gio_action_map
WHERE  
	( act_name LIKE '%金融服务%'
      AND (platform = 'iOS'
      OR platform = 'Android')
      AND datediff(first_lead_time,ts) <= 30)
GROUP BY device_id
union all
--过去45天点击金融服务次数;

SELECT 
	device_id, 
	count(*) AS finance,
	45 AS type
FROM 
	marketing_modeling.gio_action_map
WHERE
	( act_name LIKE '%金融服务%'
      AND (platform = 'iOS'
      OR platform = 'Android')
      AND datediff(first_lead_time,ts) <= 45)
GROUP BY device_id
union all
SELECT 
	device_id, 
	count(*) AS finance,
	7 AS type
FROM 
	marketing_modeling.gio_activity_map
WHERE  
	( activity LIKE '%金融服务%'
	  AND ts < first_lead_time
      AND datediff(first_lead_time,ts) <= 7)
GROUP BY device_id
union all
--过去15天点击金融服务次数;
SELECT 
	device_id, 
	count(*) AS finance,
	15 AS type
FROM 
	marketing_modeling.gio_activity_map
WHERE  
	( activity LIKE '%金融服务%'
	  AND ts < first_lead_time
      AND datediff(first_lead_time,ts) <= 15)
GROUP BY device_id
union all
--过去30天点击金融服务次数;

SELECT 
	device_id, 
	count(*) AS finance,
	30 AS type
FROM 
	marketing_modeling.gio_activity_map
WHERE  
	( activity LIKE '%金融服务%'
      AND datediff(first_lead_time,ts) <= 30)
GROUP BY device_id
union all
--过去45天点击金融服务次数;

SELECT 
	device_id, 
	count(*) AS finance,
	45 AS type
FROM 
	marketing_modeling.gio_activity_map
WHERE  
	( activity LIKE '%金融服务%'
      AND datediff(first_lead_time,ts) <= 45)
GROUP BY device_id)a
group by a.device_id,a.type;