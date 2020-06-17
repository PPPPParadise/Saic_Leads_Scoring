set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

drop table if exists marketing_modeling.gio_tmp_login_tb;
create table marketing_modeling.gio_tmp_login_tb as 
--30天内客户在App/官网登录总次数;

select
    device_id,
    count(*) as login
from rdtwarehouse.cdm_growingio_action_hma
where 
    act_name like '%登录%' and 
	act_name like '%按钮' and
    datediff(current_timestamp(),ts)<=30 and 
	applicationname LIKE '%MG%'
group by device_id;