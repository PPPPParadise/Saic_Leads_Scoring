drop table if exists marketing_modeling.tmp_buying_rate;
create table marketing_modeling.tmp_buying_rate as 
select 
s3.mobile,
s3.buying_rate
from
(
	select
		s2.*,
		row_number() over (partition by s2.mobile order by s2.buying_rate desc) as num 
	from
	(
		select
			s1.mobile,
			case when s1.c_age < 35 and s1.city_level < 3 and s1.prob_tag is not null and s1.leads_count > 0 and s1.visit is null then 10
			when  s1.c_age < 35 and s1.city_level < 3 and s1.prob_tag = '低' and s1.visit >= 1 then 30
			when  s1.c_age < 35 and s1.city_level < 3 and s1.prob_tag = '中' and s1.visit >= 1 then 50
			when  s1.c_age < 35 and s1.city_level < 3 and s1.prob_tag = '高' and s1.visit = 1 then 70
			when  s1.c_age < 35 and s1.city_level < 3 and s1.prob_tag = '高' and s1.visit > 1 then 90
			when  s1.c_age < 35 and s1.city_level = 3 and s1.prob_tag in ('低','中') and s1.leads_count > 0 and s1.visit is null then 10
			when  s1.c_age < 35 and s1.city_level = 3 and s1.prob_tag = '高' and s1.leads_count > 0 and s1.visit is null then 30
			when  s1.c_age < 35 and s1.city_level = 3 and s1.prob_tag in ('低','中') and s1.visit >= 1 then 50
			when  s1.c_age < 35 and s1.city_level = 3 and s1.prob_tag = '高' and s1.visit = 1 then 70
			when  s1.c_age < 35 and s1.city_level = 3 and s1.prob_tag = '高' and s1.visit > 1 then 90
			when  s1.c_age >= 35 and s1.city_level < 3 and s1.prob_tag in ('低','中') and s1.leads_count > 0 and s1.visit is null then 10
			when  s1.c_age >= 35 and s1.city_level < 3 and s1.prob_tag = '高' and s1.leads_count > 0 and s1.visit is null then 30
			when  s1.c_age >= 35 and s1.city_level < 3 and s1.prob_tag in ('低','中') and s1.visit >= 1 then 50
			when  s1.c_age >= 35 and s1.city_level < 3 and s1.prob_tag = '高' and s1.visit = 1 then 70
			when  s1.c_age >= 35 and s1.city_level < 3 and s1.prob_tag = '高' and s1.visit > 1 then 90	
			when  s1.c_age >= 35 and s1.city_level = 3 and s1.prob_tag = '低' and s1.leads_count > 0 and s1.visit is null then 10
			when  s1.c_age >= 35 and s1.city_level = 3 and s1.prob_tag in ('中','高') and s1.leads_count > 0 and s1.visit is null then 30
			when  s1.c_age >= 35 and s1.city_level = 3 and s1.prob_tag = '低' and s1.visit >= 1 then 50
			when  s1.c_age >= 35 and s1.city_level = 3 and s1.prob_tag in ('中','高') and s1.visit = 1 then 70
			when  s1.c_age >= 35 and s1.city_level = 3 and s1.prob_tag in ('中','高') and s1.visit > 1 then 90
			end as buying_rate
		from
			(
				select  a.mobile,
					cast((case when a.c_age is null or a.c_age = '' then 0 else substr(a.c_age,0, 2) end) as int) as c_age,
					c.prob_tag,
					b.city_name,
					case when b.city_level is null or b.city_level in ('三线城市','四线城市','五线城市') then 3
					WHEN b.city_level = '二线城市' THEN 2
					else 1 end as city_level,
					a.d_visit_ttl as visit,
					a.d_leads_count as leads_count
				from
				(select * from  marketing_modeling.app_big_wide_info where pt = '${pt}') a
				left join
				marketing_modeling.edw_city_level b
				on 
				a.c_city = b.city_name
				left join 
				(select * from marketing_modeling.app_model_application where pt='${pt}') c
				on a.mobile = c.mobile	
			) s1
	) s2
	where 
		s2.buying_rate is not null 
		
) s3
where
	s3.num = 1