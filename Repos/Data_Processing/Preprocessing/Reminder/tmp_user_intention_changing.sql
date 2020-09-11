-- 意向级别变化预警
set tez.queue.name=${queuename};
drop table if exists marketing_modeling.tmp_user_intention_changing;
create table if not exists marketing_modeling.tmp_user_intention_changing as
with t1 as	(
select 	mobile, 
		case when prob_tag = '高' then 3
		when prob_tag = '中' then 2
		when prob_tag = '低' then 1
		else null end as prob_tag
 from marketing_modeling.app_model_application
where pt = '${yesterday}'
),
t2 as (
select 	mobile, 
		case when prob_tag = '高' then 3
		when prob_tag = '中' then 2
		when prob_tag = '低' then 1
		else null end as prob_tag
 from marketing_modeling.app_model_application
where pt = '${today}'
)
select 	t1.mobile,t1.prob_tag as y,t2.prob_tag as t
from t1, t2
where  t1.mobile = t2.mobile
