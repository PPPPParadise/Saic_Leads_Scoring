DROP TABLE IF EXISTS `marketing_modeling.edw_mkt_userprofile`;
CREATE EXTERNAL TABLE if not exists `marketing_modeling.edw_mkt_userprofile`(
     mobile                      string COMMENT  "手机"
    ,sex                         string COMMENT  "性别"
    ,city                        string COMMENT  "城市"
    ,age                         string COMMENT  "年龄"
    ,focus_brand                 string COMMENT  "关注品牌（荣威RX5 MARVEL X等）"
    ,fucus_model                 string COMMENT  "关注车型（SUV，轿车 等）"
    ,have_car                    int    COMMENT  "是否有车"
    ,visited                     int    COMMENT  "是否到店"
    ,trail_booking_ttl           string COMMENT  "预定试驾总次数"
    ,last_trail_time             string COMMENT  "最近试驾时间"
    ,car_model_ttl               string COMMENT  "留资车系数量"
    ,no_deal                     string COMMENT  "未成交"
    ,buying_goal                 string COMMENT  "购买目的"
    ,loan_ppt                    string COMMENT  "贷款概率"
    ,car_type                    string COMMENT  "车型"
    ,config_prefer               string COMMENT  "配置偏好"
    ,change_and_buy_ppt          string COMMENT  "是否换购概率"
    ,focus_models                string COMMENT  "关注车系"
    ,budget                      string COMMENT  "预算"
    ,model_nums                  string COMMENT  "关注竞品数量"
    ,prob		                 string COMMENT  "意向度"
    ,outbound                    int    COMMENT  "是否外呼"
	,trail_attend_ttl			 int 	COMMENT  "试驾次数"
	,visit_ttl					 int 	COMMENT  "到店次数"
	,buying_rate				 int	COMMENT  "购车阶段（百分比）"
	,device_id					 string
	,union_id					 string
	,cookies					 string
	,fail_reason_fir_level		 array<string> COMMENT  "战败一级原因"
	,fail_reason_sec_level 		 array<string>	COMMENT  "战败二级原因"
	
	)
PARTITIONED BY(`pt`  string)
ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
	  'field.delim'='\t', 
	  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;
