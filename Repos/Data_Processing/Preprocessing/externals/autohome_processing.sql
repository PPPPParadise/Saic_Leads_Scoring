---- 创建汽车之家的小宽表，清洗重复数据
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE IF EXISTS marketing_modeling.mm_autohome_behavior;
CREATE TABLE marketing_modeling.mm_autohome_behavior as
SELECT * FROM 
(
	SELECT 
		a.*,
		ROW_NUMBER() OVER(PARTITION BY a.id ORDER BY a.leads_create_time desc) AS rn
	FROM 
		marketing_modeling.tmp_mm_autohome_behavior a 
) a1 
where a1.rn = 1
;
