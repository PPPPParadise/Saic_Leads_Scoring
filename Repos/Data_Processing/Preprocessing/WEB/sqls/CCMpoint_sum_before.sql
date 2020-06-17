set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.ccmpoints_sum_before;
create table marketing_modeling.ccmpoints_sum_before as 


select 
    a.uid as uid,
    sum(a.points) as points_before,
    "Increase" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record a
	left join
	(select uid, cust_id from rdtwarehouse.edw_cdp_id_mapping_overview group by uid,cust_id) b
	on a.uid = b.uid
	left join
    (select 
        cust_id,
        max(last_date) as last_date
     from marketing_modeling.result_last_date 
     group by cust_id) c
    on b.cust_id = c.cust_id
where 
    a.action = "INCREASE" and 
	a.created_date <= c.last_date
group by a.uid 

union all

select 
    a.uid as uid,
    sum(a.points) as points_before,
    "Decrease" as action
from 
    rdtwarehouse.ods_ccmpoint_points_record a
	left join
	(select uid, cust_id from rdtwarehouse.edw_cdp_id_mapping_overview group by uid,cust_id) b
	on a.uid = b.uid
	left join
    (select 
        cust_id,
        max(last_date) as last_date
     from marketing_modeling.result_last_date 
     group by cust_id) c
    on b.cust_id = c.cust_id
where 
    a.action = "DECREASE" and 
	a.created_date <= c.last_date
group by a.uid ;