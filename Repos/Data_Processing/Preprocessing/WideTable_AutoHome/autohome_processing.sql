---- 创建汽车之家的小宽表，清洗重复数据
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

DROP TABLE IF EXISTS marketing_modeling.app_autohome_wide_info;
CREATE TABLE marketing_modeling.app_autohome_wide_info as
SELECT * FROM 
(
	SELECT 
		a.*,
		ROW_NUMBER() OVER(PARTITION BY a.id ORDER BY a.leads_create_time desc) AS rn
	FROM 
		marketing_modeling.tmp_autohome_behavior a 
	WHERE id regexp "^[1][35678][0-9]{9}$"
) a1 
WHERE a1.rn = 1
;
