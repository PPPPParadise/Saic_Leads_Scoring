set tez.queue.name=${queuename};
drop table if exists marketing_modeling.tmp_user_leaving_alert;
--流失预警

create table marketing_modeling.tmp_user_leaving_alert as 
with following_user as (
select * from marketing_modeling.app_big_wide_info
where pt='${pt}' and 
	d_deal_flag is null and
	mobile regexp "^[1][3-9][0-9]{9}$"
),
intention as (
	select 
		a.mobile 
	from 
		marketing_modeling.tmp_user_intention_changing a, 
		following_user b
	where 
		a.t = 1 and 
		a.y = 3 and 
		a.mobile = b.mobile
),
wechat_focus AS (
	SELECT a.mobile
	FROM 
	(
		SELECT 	unifyid.id as mobile
		FROM 
			cdp.cdp_tags_combine
		WHERE 	
			dt = '${pt}' and 
			unifyid.type='p' and
			array_contains(tagrecord.tags.tagid, 180388626434)
	)a,
	(
		SELECT 	unifyid.id as mobile
		FROM 
			cdp.cdp_tags_combine
		WHERE 	
			dt = '${yesterday}' and 
			unifyid.type='p' and
			!array_contains(tagrecord.tags.tagid, 180388626433)
	)b
	WHERE
		a.mobile = b.mobile
),
autohome_car as (
select mobile from following_user
where h_models not like '%名爵%'
),
browsing as (
select mobile 
from following_user
where mobile in (
	select phone as mobile 
	from marketing_modeling.tmp_ttl_browse_time
	where dur = 0) 
),
visiting as (
select mobile from following_user
where datediff(current_timestamp(), concat(from_unixtime(unix_timestamp(cast(c_last_lead_time as string),'yyyymmdd'),'yyyy-mm-dd'),' 00:00:00'))=30 and
	c_visit_count = 0
),
followup_three_weeks as (
select a.mobile
from (
	select 	mobile,
			concat(from_unixtime(unix_timestamp(cast(c_last_lead_time as string),'yyyymmdd'),'yyyy-mm-dd'),' 00:00:00') as c_last_lead_time 
	from following_user
	where c_last_lead_time is not null
	) a
where datediff(current_timestamp(), a.c_last_lead_time) = 21
),
followup_visiting as (
select a.*,
		b.last_time
from
	(
	select 	mobile,
			concat(from_unixtime(unix_timestamp(cast(c_last_lead_time as string),'yyyymmdd'),'yyyy-mm-dd'),' 00:00:00') as c_last_lead_time 
	from following_user
	where c_last_lead_time is not null
	) a, marketing_modeling.tmp_dlm_hall_flow_processing b
	where a.mobile = b.mobile and 
		datediff(current_timestamp(), b.last_time) = 7 and
		(a.c_last_lead_time is null or datediff(current_timestamp(), a.c_last_lead_time) > 7)
),
followup_trailing as (
select a.*
from
	(
	select 	mobile,
	concat(from_unixtime(unix_timestamp(cast(c_last_trail_time as string),'yyyymmdd'),'yyyy-mm-dd'),' 00:00:00') as c_last_trail_time,
	concat(from_unixtime(unix_timestamp(cast(c_last_lead_time as string),'yyyymmdd'),'yyyy-mm-dd'),' 00:00:00') as c_last_lead_time 
	from following_user
	
	) a
	where datediff(current_timestamp(), a.c_last_trail_time) = 7 and
	(a.c_last_lead_time is null or datediff(current_timestamp(), a.c_last_lead_time) > 7)
)

select a.mobile,
		case when b.mobile is not null then 1 else 0 end as intention_decrease,
		case when c.mobile is not null then 1 else 0 end as wechat_unfollowed,
		case when d.mobile is not null then 1 else 0 end as focus_diff_car,
		case when e.mobile is not null then 1 else 0 end as stop_browsing,
		case when f.mobile is not null then 1 else 0 end as no_visiting,
		case when g.mobile is not null then 1 else 0 end as no_follow_afr_visit,
		case when h.mobile is not null then 1 else 0 end as no_follow_afr_trail,
		case when i.mobile is not null then 1 else 0 end as no_follow_afr_3weeks
from following_user a
left join intention b on a.mobile = b.mobile
left join wechat_focus c on a.mobile = c.mobile
left join autohome_car d on a.mobile = d.mobile
left join browsing e on a.mobile = e.mobile
left join visiting f on a.mobile = f.mobile
left join followup_three_weeks g on a.mobile = g.mobile
left join followup_visiting h on a.mobile = h.mobile
left join followup_trailing i on a.mobile = i.mobile



