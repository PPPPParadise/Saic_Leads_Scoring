set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set mapreduce.job.queuename=${queuename};

drop table if exists marketing_modeling.tmp_cdp_2_tb;
create table marketing_modeling.tmp_cdp_2_tb as 
select 
	b.phone as id,
	a.tagid,
	a.score,
	a.tag_name,
	a.display,
	a.father_id
from 
	marketing_modeling.tmp_cdp_tb a
left join
	cdp.edw_cdp_id_mapping_overview b
on
	a.id= b.union_id and
	a.type='u' 
where
	b.phone is not null
union all
select 
	b.phone as id,
	a.tagid,
	a.score,
	a.tag_name,
	a.display,
	a.father_id
from 
	marketing_modeling.tmp_cdp_tb a
left join
	cdp.edw_cdp_id_mapping_overview b
on
	 a.id= b.cookies and
	 a.type='c' 
where
	 b.phone is not null
union all
select 
	b.phone as id,
	a.tagid,
	a.score,
	a.tag_name,
	a.display,
	a.father_id
from 
	marketing_modeling.tmp_cdp_tb a
left join
	cdp.edw_cdp_id_mapping_overview b
on
	 a.id= b.device_id and
	 a.type='d' 
where
	 b.phone is not null
union all
select
	a.id,
	a.tagid,
	a.score,
	a.tag_name,
	a.display,
	a.father_id
from 
	marketing_modeling.tmp_cdp_tb a
where 
	a.type = 'p' and id is not null