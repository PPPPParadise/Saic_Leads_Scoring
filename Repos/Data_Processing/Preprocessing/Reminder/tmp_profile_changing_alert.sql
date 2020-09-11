-- 战败挽回提醒
drop table if exists marketing_modeling.tmp_profile_changing_alert;
create table if not exists marketing_modeling.tmp_profile_changing_alert AS
WITH browsing AS (
select 
		b.phone, 
		sum(a.duration) as dur
	from 
		(select * from dtwarehouse.cdm_growingio_activity_hma where 
		datediff(current_timestamp(), ts) =1 and
		applicationname LIKE '%MG%') a
	join  cdp.edw_cdp_id_mapping_overview b
	on b.pt='${pt}' and a.device_id = b.device_id 
	where b.phone regexp "^[1][3-9][0-9]{9}$" 
	GROUP BY b.phone
),
wechat AS (
	SELECT a.mobile
	FROM 
	(
		SELECT 	unifyid.id as mobile
		FROM 
			cdp.cdp_tags_combine
		WHERE 	
			dt = '${pt}' and 
			unifyid.type='p' and
			array_contains(tagrecord.tags.tagid, 180388626433)
	)a,
	(
		SELECT 	unifyid.id as mobile
		FROM 
			cdp.cdp_tags_combine
		WHERE 	
			dt = '${yesterday}' and 
			unifyid.type='p' and
			!array_contains(tagrecord.tags.tagid, 180388626434)
	)b
	WHERE
		a.mobile = b.mobile
)
SELECT 
	a.mobile,
	CASE WHEN 	(b.prob_tag ='高') OR
				(c.phone is not null ) OR
				(d.mobile is not null) THEN 1 ELSE 0 END AS to_golden_dealer,
	CASE WHEN 	((DATEDIFF(current_timestamp(),d_last_leads_time)) <= 14
				OR 
				(DATEDIFF(current_timestamp(),d_fir_leads_time) <= 14)) OR
				((DATEDIFF(current_timestamp(),d_fir_card_time) <= 14) 
				OR
				(DATEDIFF(current_timestamp(),last_cust_time) <= 14))
	THEN 1 ELSE 0 END AS  to_delivered_dealer
FROM (
	SELECT 
			mobile,
			d_deal_flag,
			d_last_leads_time,
			d_last_dealfail_d,
			d_fir_leads_time,
			d_fir_card_time,
			concat(FROM_unixtime(unix_timestamp(cast(c_last_cust_base_time as string),'yyyymmdd'),'yyyy-MM-dd'),' 00:00:00') AS last_cust_time 
	FROM marketing_modeling.app_big_wide_info WHERE pt='${pt}' AND d_deal_flag = 0) a
LEFT JOIN
(SELECT * FROM marketing_modeling.app_model_application WHERE pt='${pt}') b
on a.mobile = b.mobile
LEFT JOIN
browsing c
on a.mobile = c.phone AND c.dur > 0
LEFT JOIN wechat d
on a.mobile = d.mobile

