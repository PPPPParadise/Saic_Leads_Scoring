set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.ccmpoints_all_features;
create table marketing_modeling.ccmpoints_all_features as 


select 
	a1.phone as phone,
    a1.uid as uid,
    b.points as points_increase_15, --过去15天取得积分总数
	c.points as points_increase_30, --过去30天取得积分总数
	d.points as points_increase_45, --过去45天取得积分总数
	e.points as points_decrease_15,	--过去15天使用积分总数
	f.points as points_decrease_30, --过去30天使用积分总数
	g.points as points_decrease_45, --过去45天使用积分总数
	h.points_before as points_increase_pre_status, --战败/成交前总获取积分数
	i.points_before as points_decrease_pre_status  --战败/成交前总获取积分数
from 
    (select phone,uid
	  from 
	  rdtwarehouse.edw_cdp_id_mapping_overview
	  where phone is not null and
			phone != '' and
			uid is not null and
			uid != ''
	  group by phone,uid ) a1
left join
	(select 
		distinct(uid)
	 from 
		rdtwarehouse.ods_ccmpoint_points_record
		where uid is not null) a
on a1.uid = a.uid		
left join
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 15 and
		   action = "Increase") b
on a.uid = b.uid
left join 
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 30 and
		   action = "Increase") c
on a.uid = c.uid
left join 
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 45 and
		   action = "Increase") d
on a.uid = d.uid
left join 
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 15 and
		   action = "Decrease") e
on a.uid = e.uid
left join 
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 30 and
		   action = "Decrease") f
on a.uid = f.uid
left join 
	(select uid, points from marketing_modeling.ccmpoints_sum
	 where type = 45 and
		   action = "Decrease") g
on a.uid = g.uid
left join 
	(select uid, points_before from marketing_modeling.ccmpoints_sum_before
	 where action = "Increase") h
on a.uid = h.uid
left join 
	(select uid, points_before from marketing_modeling.ccmpoints_sum_before
	 where action = "Decrease") i
on a.uid = i.uid





