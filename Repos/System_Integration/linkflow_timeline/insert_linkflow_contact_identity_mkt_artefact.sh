#!/bin/bash
#/*********************************************************************
#*模块名  : mkt渠道
#*程序名  : insert_linkflow_contact_identity_mkt_artefact.sh
#*功能    : 从宽表中select数据到linkflow的contact_identity表
#*开发人  : 马昂
#*开发日期: 2020-06-23
#*修改记录: 
#*          
#*
#*
#*
#*********************************************************************/

pt=$3
channel="7"
sql="
insert into table linkflow.contact_identity partition (pt='${pt}')
select 
	s2.id,
	s2.version,
	null as anonymous_id,
	s2.channel_id,
	s2.contact_id,
	s2.date_created,
	null as email,
	s2.is_active,
	s2.last_updated,
	null as mobile_phone,
	s2.tenant_id,	
	null as nickname,
	s2.original_id,
	s2.external_id 
from (
	select 
		s1.id,
		s1.version,
		s1.channel_id,
		s1.contact_id,
		s1.date_created,
		s1.is_active,
		s1.last_updated,
		s1.tenant_id,
		s1.original_id,
		s1.external_id 
	from (
		select 
			concat((hash(t2.id) & 2147483647) , substr(t1.mobile,7),'$channel') id,
			1 version,
			'$channel' channel_id,
			concat((hash(t2.id) & 2147483647) , substr(t1.mobile,7)) contact_id,
			FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') date_created,
			0 is_active,
			FROM_UNIXTIME(UNIX_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss') last_updated,
			1 tenant_id,
			1 original_id,
			t1.mobile as external_id 
			from 
			(select * from marketing_modeling.edw_mkt_userprofile where pt='${pt}') t1 
			inner join 
			(select * from cdp.customer_info_withID_join where pt='${pt}' and id is not null) t2
			on t1.mobile=t2.id 
			) s1
	group by 
		s1.id,
		s1.version,
		s1.channel_id,
		s1.contact_id,
		s1.date_created,
		s1.is_active,
		s1.last_updated,
		s1.tenant_id,
		s1.original_id,
		s1.external_id
) s2;
"
hive -hivevar pt=$pt -e "$sql" 
