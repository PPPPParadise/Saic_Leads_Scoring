set tez.queue.name=${queuename};

INSERT OVERWRITE TABLE `marketing_modeling.app_timeline_behavior` PARTITION(pt='${pt}')
select 
	aa.mobile,
	aa.media,
	aa.creative,
	aa.last_trail_time,
	aa.browse_time_d7,
	aa.finance_click
from (
	select 
	a.mobile,
	concat ('{\"info\":\"',b.media, '\",\"dt\":\"', b.reqtime, '\"}') as media,
	concat ('{\"info\":\"',b.creative, '\",\"dt\":\"', b.reqtime, '\"}') as creative,
	concat ('{\"info\":\"',a.c_last_trail_time, '\",\"dt\":\"', a.c_last_trail_time, '\"}') as last_trail_time,
	concat ('{\"info\":\"',c.ttl_d7, '\",\"dt\":\"', current_timestamp(), '\"}') as browse_time_d7,
	concat ('{\"info\":\"',1, '\",\"dt\":\"', d.ts, '\"}') as finance_click
	FROM (select a.mobile,a.c_last_trail_time from marketing_modeling.app_big_wide_info where pt='${pt}') a
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
or aa.finance_click is not null 
or aa.last_trail_time is not null 
or aa.media is not null
