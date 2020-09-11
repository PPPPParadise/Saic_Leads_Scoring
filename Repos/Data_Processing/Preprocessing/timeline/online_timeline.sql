set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
set tez.queue.name=${queuename};
WITH user_base AS (
	SELECT aa.phone,aa.act_name,aa.ts
	FROM (
	SELECT b.phone,a.act_name,a.ts 
	FROM (SELECT * FROM dtwarehouse.cdm_growingio_action_hma WHERE applicationname LIKE '%MG%') a
	LEFT JOIN  
		(SELECT device_id,
				phone 
		FROM cdp.edw_cdp_id_mapping_overview 
		WHERE pt='${pt}' and phone regexp "^[1][3-9][0-9]{9}$") b 
	ON a.device_id = b.device_id
	) aa
	WHERE
		aa.phone IS NOT NULL
),
second_hand AS (
	SELECT 	phone,
			ts,
			1 as event_id,
			row_number() over (PARTITION BY phone ORDER BY ts desc) as rn
	FROM user_base
	WHERE act_name = 'mg_top_navigation_usedcarcertification_click'
	
),
purchase AS (
	SELECT 	phone,
			ts,
			2 as event_id,
			row_number() over (PARTITION BY phone ORDER BY ts desc) as rn
	FROM user_base
	WHERE act_name in ('mg_seriespage_purchase_click',
					'mg_sidebar_purchase_click',
					'上汽名爵-安卓-爱车-订购爱车按钮',
					'订购爱车')
),
finance AS (
	SELECT 	phone,
			ts,
			3 as event_id,
			row_number() over (PARTITION BY phone ORDER BY ts desc) as rn
	FROM user_base
	WHERE act_name in ('mg_financialservice_click',
					'上汽名爵-ios-爱车-金融服务1',
					'上汽名爵-安卓-爱车-金融服务-1')
),
trail AS (
	SELECT 	phone,
			ts,
			4 as event_id,
			row_number() over (PARTITION BY phone ORDER BY ts desc) as rn
	FROM user_base
	WHERE act_name in ('mg_seriespage_testdrive_click',
	'mg_seriespage_bottom_testdrive_click',
	'mg_seriespage_suspension_testdrive_click',
	'mg_testdrivepage_testdrive_click',
	'mg_dealersearchpage_testdrive_click',
	'试乘试驾',
	'上汽名爵-安卓-爱车-试乘试驾按钮',
	'上汽名爵-安卓-我的-试乘试驾按钮',
	'上汽名爵-ios-我的-试乘试驾按钮',
	'上汽名爵-ios-爱车-试乘试驾按钮')
),
-- event id 点击竞品5,
--			点击MG6,
--			再次访问汽车之家7，
--			购车预算8			
autohome as(
	SELECT
		id as phone,
		case when models like '%名爵%' then 1 else 0 end as focus_mg,
		budget,
		leads_create_time,
		row_number() over (PARTITION BY id ORDER BY leads_create_time desc) as rn
	FROM
		marketing_modeling.app_autohome_wide_info
	WHERE leads_create_time is not null
),
visiting_medias as (
	SELECT
		mobile,
		update_time,
		leads_resource_type ,
		row_number() over (PARTITION BY mobile ORDER BY update_time desc) as rn
	FROM 
		dtwarehouse.ods_dlm_t_potential_customer_leads_m 
	WHERE pt = '${pt}' and 
		mobile regexp "^[1][3-9][0-9]{9}$" and 
		leads_resource_type != 4
)
INSERT OVERWRITE TABLE marketing_modeling.app_mkt_timeline partition (pt='${pt}')
SELECT
	null as id,
	a.mobile,
	-1 as dealer_id,
	null as event_id,
	concat_ws(',', collect_set(concat (a.event_id, '|', a.event_time,'|', a.event_info))) as event_info,
	null as event_time,
	from_unixtime((unix_timestamp()+8*60*60)) as create_time,
		null as attr1,
		null as attr2,
		null as attr3
FROM
(
	SELECT 
		null as id,
		second_hand.phone as mobile,
		-1 as dealer_id,
		second_hand.event_id as event_id,
		''as event_info,
		second_hand.ts as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM second_hand
	WHERE
		second_hand.rn = 1
	UNION ALL
	SELECT 
		null as id,
		purchase.phone as mobile,
		-1 as dealer_id,
		purchase.event_id as event_id,
		''as event_info,
		purchase.ts as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM purchase
	WHERE
		purchase.rn = 1
	UNION ALL
	SELECT 
		null as id,
		finance.phone as mobile,
		-1 as dealer_id,
		finance.event_id as event_id,
		''as event_info,
		finance.ts as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM finance
	WHERE
		finance.rn = 1
	UNION ALL
	SELECT 
		null as id,
		trail.phone as mobile,
		-1 as dealer_id,
		trail.event_id as event_id,
		''as event_info,
		trail.ts as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM trail
	WHERE trail.rn = 1
	UNION ALL
	SELECT 
		null as id,
		autohome.phone as mobile,
		-1 as dealer_id,
		5 as event_id,
		''as event_info,
		leads_create_time as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM
		autohome
	WHERE
		focus_mg = 1 and
		rn = 1
	UNION ALL
	SELECT 
		null as id,
		autohome.phone as mobile,
		-1 as dealer_id,
		6 as event_id,
		''as event_info,
		leads_create_time as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM
		autohome
	WHERE
		focus_mg = 0 and
		rn = 1
	UNION ALL
	SELECT 
		null as id,
		autohome.phone as mobile,
		-1 as dealer_id,
		8 as event_id,
		budget as event_info,
		leads_create_time as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM
		autohome
	WHERE
		budget is not null and
		rn = 1
	UNION ALL
	SELECT 
		null as id,
		visiting_medias.mobile as mobile,
		-1 as dealer_id,
		7 as event_id,
		''as event_info,
		update_time as event_time,
		current_timestamp() as create_time,
		null as attr1,
		null as attr2,
		null as attr3
	FROM
		visiting_medias
	WHERE
		rn = 1
) a
GROUP BY
	a.mobile
