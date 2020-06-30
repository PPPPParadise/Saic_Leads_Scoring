set tez.queue.name=${queuename};
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;

DROP TABLE if EXISTS marketing_modeling.app_cdp_wide_info;
CREATE TABLE marketing_modeling.app_cdp_wide_info as
SELECT 
	id,
	-- '%MG是否注册APP'
	sum(case when father_id='261993005056' then score end) as is_app_user,
	max(case when display like '%MG最后一次浏览官网日期%' then substr(cast(cast(score as bigint) as string),0,8) end) as last_browse_time,
	sum(case when display like '%MG过去14天浏览官网页面次数' then cast(score as int) end) as brwose_count_d14,
	sum(case when display like '%MG过去14天访问APP平均时长' then score end) as avg_browse_time_d14,
	max(case when display like '%MG最近一次访问APP时间' then substr(cast(cast(score as bigint) as string),0,8) end) as last_app_visit_time,
	sum(case when display like '%MG过去14天取得积分总数' then score end) as app_point_ttl_d14,
	sum(case when display like '%MG总使用积分次数' then score end) as app_point_ttl,
	max(case when display like '%MG最近一次使用积分时间' then cast(score as int) end) as last_app_point_use_time,
	--display like '%MG是否试乘试驾'
	sum(case when tag_name like '%是否试乘试驾%' and tag_name like '%MG%' then score end) as is_trail_user,
	-- '试驾车系' --532575944704
	concat_ws('|',collect_set(case when tag_name like '%试驾车系%' and tag_name like '%MG%' then display end)) as trail_vel,
	
	-- '最近一次试驾车系'
	concat_ws('|',collect_set(case when tag_name like '%最近一次试驾车系%' and tag_name like '%MG%' then display end)) as last_trail_vel,
	
	--'购买车系'566935683072
	concat_ws('|',collect_set(case when tag_name like '%购买车系%' and tag_name like '%MG%' then display end)) as deliver_vel,
	
	sum(case when display like '%MG总到店次数' then score end) as visit_count,
	sum(case when display like '%MG过去14天到店次数' then score end) as visit_count_d14,
	max(case when display like '%MG成交日期%' then cast(score as int) end) as deal_time,
	max(case when display like '%MG最近一次试驾日期%' then score end) as last_trail_time,
	max(case when display like '%MG最近跟进日期%' then cast(score as int) end) as last_lead_time,
	sum(case when display like '%MG过去14天预约试乘试驾次数' then score end) as trail_count_d14,
	--display like '%MG最近触达平台'
	concat_ws('|',collect_set(case when father_id='481036337152' then display end)) as last_reach_platform,
	--display like '线索来源'
	concat_ws('|',collect_set(case when father_id='912212398964736' then display end)) as lead_sources,
	--没有%MG查看论坛时间标签，只有ROEWE
	max(case when display like '%MG最近一次查看资讯论坛时间' then substr(cast(cast(score as bigint) as string),0,8) end) as last_bbs_time,
	sum(case when display like '%MGAPP下订日期' then substr(cast(cast(score as bigint) as string),0,8) end) as order_time,
	sum(case when display like '%MG上次外呼结果' then score end) as last_sis_result,
	sum(case when display like '%MG是否打开短链' then score end) as open_link,
	max(case when display like '%MG最近一次进店日期' then cast(score as int) end) as last_visit_time,
	max(case when display like '%MG最近参加活动日期' then substr(cast(cast(score as bigint) as string),0,8) end) as last_activity_time,
	sum(case when display like '%MG跨店建卡数量' then score end) as create_card_count,
	sum(case when display like '%MG客户寿命' then score end) as cust_live_period,
	sum(case when display like '%MG历史跟进次数' then score end) as followup_ttl,
	max(case when display like '%MG上次外呼日期' then substr(cast(cast(score as bigint) as string),0,8) end) as last_sis_time,
	concat_ws('|', collect_set(case when display like '%MG战败日期' then substr(cast(cast(score as bigint) as string),0,8) end)) as deal_fail_time,
	--display like '%MG留资车系'
	concat_ws('|',collect_set(case when father_id='912186629160960' then display end)) as lead_model,
	sum(case when display like '%MG战败核实状态' then score end) as fail_status,
	sum(case when display like '%MGRX5 MAX视频观看记录' then score end) as rx5_video_time,
	sum(case when display like '%MGMARVELX视频观看记录' then score end) as marvel_video_time,
	concat_ws('|', collect_set(case when display like '%性别%' then display end)) as sex,
	concat_ws('|', collect_set(case when tag_name like '%年龄%' then display end)) as age,
	concat_ws('|',collect_set(case when tag_name like '%个人地址%' and size(split(tag_name, '/')) == 6 then display end)) as province,
	concat_ws('|',collect_set(case when  tag_name like '%个人地址%' and size(split(tag_name, '/')) == 7 then display end)) as city,
	sum(case when display like '%MG最近流失预警类型' then score end) as last_lost_type,
	max(case when display like '%MG最近流失预警日期' then substr(cast(cast(score as bigint) as string),0,8) end) as last_lost_time,
	sum(case when display like '%MG是否浏览爱车展厅了解详情车型及配置页' then score end) as roewe_brwose_homepage,
	sum(case when display like '%MG是否搜索过车型关键词' then score end) as search_vel,
	sum(case when display like '%MG是否观看过车型的资讯' then score end) as browse_model_info,
	sum(case when display like '%MG是否预约过车型试驾' then score end) as trail,
	sum(case when display like '%MG是否下订' then score end) as make_deal,
	sum(case when display like '%MG关注车系' then score end) as model,
	max(case when display like '%MGAPP注册时间' then substr(cast(cast(score as bigint) as string),0,8) end) as register_time,
	max(case when display like '%MG最近一次建卡日期' then substr(cast(cast(score as bigint) as string),0,8) end) as last_cust_base_time,
	sum(case when display like '%MG战败核实结果' then score end) as fail_real_result,
	sum(case when display like '%MG过去14天浏览APP次数' then score end) as app_browse_count_d14

FROM 
	marketing_modeling.tmp_cdp_2_tb
-- where id is not null and id != ''
WHERE id regexp "^[1][35678][0-9]{9}$"
group by id
	
	