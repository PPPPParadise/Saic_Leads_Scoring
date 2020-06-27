set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.tmp_userprofile_finance_count;
create table marketing_modeling.tmp_userprofile_finance_count as 
select b.phone,
		a.ts
		
from
(
SELECT 
	device_id, 
	ts,
	ROW_NUMBER() OVER(PARTITION BY device_id ORDER BY ts desc) AS rn
FROM 
	dtwarehouse.cdm_growingio_action_hma
WHERE  
	 act_name LIKE '%金融服务%'  
) a
left join 
	cdp.edw_cdp_id_mapping_overview b
on a.device_id = b.device_id and b.phone is not null
where rn = 1