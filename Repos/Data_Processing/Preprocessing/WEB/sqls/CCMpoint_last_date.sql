set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.result_last_date;
create table marketing_modeling.result_last_date as 

select 
c.cust_id as cust_id,
"success" as type,
max(CASE WHEN c.buy_date is null and c.buy_date > current_timestamp() then c.create_date else c.buy_date end) as last_date
from (select 
        cust_id,
        buy_date,
        cast(create_time AS date) as create_date
      from rdtwarehouse.ods_dlm_t_deliver_vel)c 
group by cust_id

union all

select 
c.cust_id as cust_id,
"fail" as type,
max(c.create_date) as last_date
from (select 
        cust_id,
        cast(create_time AS date) as create_date
      from rdtwarehouse.ods_dlm_t_oppor_fail)c 
group by cust_id ;