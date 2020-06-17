
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.cdp_tmp_tb;
create table marketing_modeling.cdp_tmp_tb as 
select 
	a.id,
	a.type,
	a.tags.tagid,
	a.tags.score,
	b.tag_name,
	b.display,
	b.father_id
from
	(select 	
		unifyid.id, 
		unifyid.type, 
		tags
	from 
		cdp.cdp_tags_combine 
	lateral view 
		explode(tagrecord.tags) tb as tags 
	where dt='${pt}'
	) a
left join
	(select * from cdp.cdp_ma_tags where dt='${pt}') b
on a.tags.tagid=b.tag_id 
where b.tag_name is not null;