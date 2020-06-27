#!/bin/bash
#/*********************************************************************
#*模块名  : mkt事件-时光轴
#*程序名  : insert_mkt_timeline_finance.sh
#*功能    : 点击金融服务
#*开发人  : 马昂
#*开发日期: 2020-06-23
#*修改记录: 
#*          
#*
#*
#*
#*********************************************************************/
pt=$3
channel="udc_19ip3JOHr"
trail="UDE_1OAVKDV0O"
finance="UDE_1W9BB8QX1"
browsing="UDE_1OZXSVUEG"
sql="
-- 点击金融/贷款服务
INSERT INTO linkflow.event partition (pt='${pt}')
SELECT  s2.id
       ,1 as version
       ,null AS anonymous_id
       ,'$channel' as channel_id
       ,'MKT渠道' as channel_name
       ,s2.contact_identity_id
       ,null as context
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as date_created
       ,'communication_stretegy' as event
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as event_date
       ,'$finance' as event_id
       ,FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') as last_updated
       ,null as property
	   ,1 as tenant_id
       ,s2.finance_click as attr1
	   ,null as attr2
       ,null as attr3
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
        (hash(a.mobile) & 376597832) as id,
        get_json_object(a.finance_click,'$info') as finance_click,
        get_json_object(a.finance_click,'$dt') as dt,
		c.id as contact_identity_id
    from
    marketing_modeling.app_timeline_behavior a
	inner join
		linkflow.contact_identity c
		on a.mobile = c.external_id
    where a.finance_click is not null and a.pt = '${pt}'
) s2;
"
hive -hivevar pt=$pt -e "$sql"