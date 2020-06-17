set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.ccmpoints_sum;
create table marketing_modeling.ccmpoints_sum as 

--过去15天取得积分总数
select 
    uid,
    sum(points) as points,
    15 as type,
    "Increase" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "INCREASE" and
    datediff(current_timestamp(),created_date)<=15
group by uid

union all

--过去30天取得积分总数
select 
    uid,
    sum(points) as points,
    30 as type,
    "Increase" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "INCREASE" and
    datediff(current_timestamp(),created_date)<=30
group by uid

union all

--过去45天取得积分总数
select 
    uid,
    sum(points) as points,
    45 as type,
    "Increase" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "INCREASE" and
    datediff(current_timestamp(),created_date)<=45
group by uid

union all
--过去15天使用积分总数
select 
    uid,
    sum(points) as points,
    15 as type,
    "Decrease" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "DECREASE" and
    datediff(current_timestamp(),created_date)<=15
group by uid

union all

--过去30天使用积分总数
select 
    uid,
    sum(points) as points,
    30 as type,
    "Decrease" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "DECREASE" and
    datediff(current_timestamp(),created_date)<=30
group by uid

union all

--过去45天使用积分总数
select 
    uid,
    sum(points) as points,
    45 as type,
    "Decrease" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record
where 
    action = "DECREASE" and
    datediff(current_timestamp(),created_date)<=45
group by uid

