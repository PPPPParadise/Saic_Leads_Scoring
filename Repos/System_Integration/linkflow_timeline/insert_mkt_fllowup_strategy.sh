#!/bin/bash
#/*********************************************************************
#*模块名  : mkt跟进建议
#*程序名  : insert_mkt_fllowup_strategy.sh
#*功能    : 导入跟进建议
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
INSERT INTO TABLE linkflow.event partition (pt='${pt}')
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
       ,s2.starting_case as attr1
       ,s2.fir_sentence as attr2
       ,s2.sec_sentence as attr3
       ,s2.thir_sentence as attr4
       ,s2.fourth_sentence as attr5
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
        '根据用户当前购车状态，推荐跟进步骤如下：' as starting_case,
        case when s1.outbound > 0 then 
        '步骤一: 确认客户身份，渠道来源' 
        when s1.visit = 1 then 
        '步骤一: 询问顾客状态' 
        when s1.trail > 0 then 
        '步骤一：通过利益诱导（试驾礼品）跟进'
        else '步骤一: 确认客户身份，渠道来源' 
        end as fir_sentence, 
         case when s1.outbound > 0 then 
        '步骤二: 挖掘客户信息（购车目的，时间与权益偏好）' 
        when s1.visit = 1 then 
        '步骤二: 根据偏好给出邀约理由' 
        when s1.trail > 0 then 
        '步骤二：活动邀请跟进'
        else '步骤二: 挖掘客户信息（购车目的，时间与权益偏好）' 
        end as sec_sentence,
        case when s1.outbound > 0 then 
        '步骤三: 利用噱头（亲子，优惠，养生等）邀约客户到店' 
        when s1.visit = 1 then 
        '步骤三: 告知店铺位置并明确再次到店时间' 
        when s1.trail > 0 then 
        '步骤三：价格行情波动为由跟进'
        else '步骤三: 利用噱头（亲子，优惠，养生等）邀约客户到店' 
        end as thir_sentence,
        case when s1.outbound > 0 then 
        '步骤四: 告知店铺位置并明确再次到店时间'
        when s1.visit <> 1 and (s1.trail < 0 or s1.trail is null) then
        '步骤四: 告知店铺位置并明确再次到店时间'
        end as fourth_sentence,
        s1.contact_identity_id
    from
    (
    select  a.mobile,
            a.outbound,
            a.trail_attend_ttl as trail,
            a.visit_ttl as visit,
			10000000000+(hash(concat(a.mobile,'$channelid'))&2147483647)  as contact_identity_id
    from    
    marketing_modeling.edw_mkt_userprofile a

	where a.pt='${pt}'
    ) s1
) s2;
"
hive -hivevar pt=$pt -e "$sql" 