
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.tmp_timeline_behavior;
create table marketing_modeling.tmp_timeline_behavior as 
select * from (
select 
a.mobile,
concat ('{\"info\":\"',b.media, '\",\"dt\":\"', b.reqtime, '\"}') as media,
concat ('{\"info\":\"',b.creative, '\",\"dt\":\"', b.reqtime, '\"}') as creative,
concat ('{\"info\":\"',a.c_last_trail_time, '\",\"dt\":\"', a.c_last_trail_time, '\"}') as last_trail_time,
concat ('{\"info\":\"',c.ttl_d7, '\",\"dt\":\"', current_timestamp(), '\"}') as browse_time_d7,
concat ('{\"info\":\"',1, '\",\"dt\":\"', d.ts, '\"}') as finnance_click
FROM marketing_modeling.mm_big_wide_info a
left join 
marketing_modeling.mm_dmp_user_info b
on a.mobile = b.phone and b.rn = 1
left join
marketing_modeling.tmp_userprofile_browse_time c
on a.mobile = c.phone
left join
marketing_modeling.tmp_userprofile_finance_count d
on a.mobile = d.phone
) aa
where aa.browse_time_d7 is not null
or aa.creative is not null 
or aa.finnance_click is not null 
or aa.last_trail_time is not null 
or aa.media is not null


select * from marketing_modeling.tmp_timeline_behavior