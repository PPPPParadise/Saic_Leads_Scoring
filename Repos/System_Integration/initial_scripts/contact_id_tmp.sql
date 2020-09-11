DROP TABLE IF EXISTS marketing_modeling.contact_identity_tmp; 
--创建记录联系人在某个渠道中账号信息表
create external table if not exists marketing_modeling.contact_identity_tmp(		
		id  bigint  COMMENT '{"columnName":"contact_identity id","columnDesc":"contact_identity 数据唯一id"}',	
		version  int  COMMENT  '{"columnName":"版本","columnDesc":"技术字段,使用默认中1即可"}',		
		anonymous_id  string  COMMENT  '{"columnName":"匿名Id","columnDesc":"微信中是 OpenId"}',
		channel_id  bigint  COMMENT  '{"columnName":"渠道表中 Id值","columnDesc":"渠道表中 Id值"}',
		contact_id  bigint  COMMENT  '{"columnName":"联系人id","columnDesc":"对应联系人中id"}',
		date_created  timestamp COMMENT  '{"columnName":"数据创建时间","columnDesc":"数据创建时间，格式:时间戳"}',
		email  string  COMMENT  '{"columnName":"邮箱","columnDesc":""}',
		is_active  int  COMMENT  '{"columnName":"是否激活","columnDesc":"比如微信中，该粉丝是否关注"}',
		last_updated  timestamp COMMENT  '{"columnName":"数据最后修改时间","columnDesc":"初次跟date_created一致，格式:时间戳"}',
		mobile_phone  string  COMMENT  '{"columnName":"手机号码","columnDesc":"对应cdp联系人中phone字段"}',
		tenant_id  bigint  COMMENT  '{"columnName":"租户id","columnDesc":"租户所属id"}',
		nickname  string  COMMENT  '{"columnName":"别名","columnDesc":"对应cdp联系人nickname字段"}',
		original_id bigint COMMENT  '{"columnName":"原联系人 Id","columnDesc":"使用默认中1即可"}',
		external_id bigint COMMENT  '{"columnName":"实名用户 Id","columnDesc":"实名用户 Id"}'
    ) COMMENT '{"tableName":"记录联系人在某个渠道中账号信息表","tableDesc":"用于记录联系人在某个渠道中账号信息",
     "jiraInfo":"","developOwner":"向导","businessOwner":"郁雅婷" }'
	 partitioned by (pt string)
     row format delimited fields terminated by '\t' stored as orc
;