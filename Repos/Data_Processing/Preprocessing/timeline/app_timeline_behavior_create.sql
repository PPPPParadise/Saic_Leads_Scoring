DROP TABLE if exists `marketing_modeling.app_timeline_behavior`;
CREATE TABLE if not exists `marketing_modeling.app_timeline_behavior`(
	  `mobile` string COMMENT "手机", 
	  `media` string  COMMENT "渠道", 
	  `creative` string  COMMENT "投放内容", 
	  `last_trail_time` string  COMMENT "最后一次试驾时间", 
	  `browse_time_d7` string  COMMENT "七天内浏览次数", 
	  `finance_click` string  COMMENT "是否点击金融服务"
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