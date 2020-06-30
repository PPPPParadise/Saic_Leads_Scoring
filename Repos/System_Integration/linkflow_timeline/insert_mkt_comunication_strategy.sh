#!/bin/bash
#/*********************************************************************
#*模块名  : mkt话术
#*程序名  : insert_mkt_comunication_strategy.sh
#*功能    : 导入话术
#*开发人  : 马昂
#*开发日期: 2020-06-22
#*修改记录: 
#*          
#*
#*
#*
#*********************************************************************/
pt=$3
channel="udc_19ip3JOHr"
channelid="7"
eventid="UDE_1T3MJ8TJ8"
sql="
INSERT INTO linkflow.event partition (pt='${pt}')
SELECT  null as id
       ,s2.version
       ,null AS anonymous_id
       ,'$channel' as channel_id
       ,'MKT渠道' as channel_name
       ,s2.contact_identity_id
       ,null as context
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as date_created
       ,'communication_stretegy' as event
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as event_date
       ,'$eventid' as event_id
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as last_updated
       ,null as property
	   ,1 as tenant_id
       ,s2.fir_sentence as attr1
       ,s2.sec_sentence as attr2
       ,s2.thir_sentence as attr3
	   ,null as attr4
	   ,null as attr5
	   ,null as attr6
	   ,null as attr7
	   ,null as attr8
	   ,null as attr9
	   ,null as attr10
	   ,null as attr11
	   ,null as attr12
	   ,null as attr13
	   ,null as attr14
	   ,null as attr15
	   ,null as attr16
	   ,null as attr17
	   ,null as attr18
	   ,null as attr19
	   ,null as attr20
	   ,null as parent_id
	   ,null as sdk_version
	   ,null as sdk_type
	   ,null as platform
	   ,null as screen_width
	   ,null as screen_height
	   ,null as app_version
	   ,null as bundle_key
	   ,null as os
	   ,null as os_version
	   ,null as browser
	   ,null as browser_version
	   ,null as country
	   ,null as province
	   ,null as city
	   ,null as network_type
	   ,null as manufacturer
	   ,null as device_model
	   ,null as operator
	   ,null as debug_mode
	   ,null as imei
	   ,null as ip
	   ,null as latitude
	   ,null as longitude
	   ,null as trigger_flow
	   ,null as channel_app_id

FROM 
(
    select 
        '1' as version,
        case when s1.age <= 35 and s1.prob_tag = '高' and s1.city_level in (1,2) then 
        '名爵是经济，便利，实惠的通勤工具。能满足人生阶段（创业奋斗，人生第一辆车，新婚准备，备孕期，小孩上学等）的需求' 
        when s1.age <= 35 and s1.prob_tag = '中' and s1.city_level in (1,2) then 
        '名爵是经济，便利，实惠的通勤工具。能提供的便利性包括保养，上牌，续航等。' 
        when s1.age <= 35 and s1.prob_tag = '低' and s1.city_level in (1,2) then 
        '名爵是经济，便利，实惠的通勤工具。是定位年轻用户，经济适用性高的品牌。' 
        when s1.age > 35 and s1.prob_tag = '高' and s1.city_level in (1,2) then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。能满足沉稳大气，功能全面的备用需求' 
        when s1.age > 35 and s1.prob_tag = '中' and s1.city_level in (1,2) then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。能提供不同的驾驶感受。' 
        when s1.age > 35 and s1.prob_tag = '低' and s1.city_level in (1,2) then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。是有奢华感，简约感的车辆品牌' 
        when s1.age <= 35 and s1.prob_tag = '高' and s1.city_level = 3 then 
        '名爵是经济，便利，实惠的通勤工具。能满足人生阶段（创业奋斗，人生第一辆车，新婚准备，备孕期，小孩上学等）的需求' 
        when s1.age <= 35 and s1.prob_tag = '中' and s1.city_level = 3 then 
        '名爵是经济，便利，实惠的通勤工具。能提供的便利性包括保养，上牌，续航等。' 
        when s1.age <= 35 and s1.prob_tag = '低' and s1.city_level = 3 then 
        '名爵是经济，便利，实惠的通勤工具。是定位年轻用户，经济适用性高的品牌。' 
        when s1.age > 35 and s1.prob_tag = '高' and s1.city_level = 3 then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。能满足沉稳大气，功能全面的备用需求' 
        when s1.age > 35 and s1.prob_tag = '中' and s1.city_level = 3 then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。能提供不同的驾驶感受。' 
        when s1.age > 35 and s1.prob_tag == '低' and s1.city_level = 3 then 
        '名爵是低调又不失奢华，简约而不简单的理想备用车。是有奢华感，简约感的车辆品牌' 
        end as fir_sentence,
        concat('此顾客偏好为:',s1.config_prefer) as sec_sentence,
        '有关MG6，主要人群为单身青年，三口之家,毕业生。请参考MG6话术库' as thir_sentence,
		s1.contact_identity_id
 
    from
    (
		select  a.mobile,
            cast((case when a.age is null or a.age = '' then 0 else a.age end) as int) as age,
            a.prob as prob_tag,
            b.city_name,
            case when b.city_level is null or b.city_level in ('三线城市','四线城市','五线城市') then 3
            WHEN b.city_level = '二线城市' THEN 2
            else 1 end as city_level,
			10000000000+(hash(concat(a.mobile,'$channelid'))&2147483647)  as contact_identity_id,
			a.config_prefer
		from
		marketing_modeling.edw_mkt_userprofile a
		left join
		marketing_modeling.edw_city_level b
		on 
		a.city = b.city_name
		where a.pt = '${pt}'
    ) s1
) s2;
"
hive -hivevar pt=$pt -e "$sql"