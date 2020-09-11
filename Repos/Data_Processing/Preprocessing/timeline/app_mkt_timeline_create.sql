DROP TABLE IF EXISTS marketing_modeling.app_mkt_timeline;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_mkt_timeline (
    id                            	   bigint      comment    '用户唯一ID，自增',
    mobile              	           string         comment    '手机号',
    dealer_id           	           string         comment    '经销商id',
    event_id            		       string         comment    '事件id', 
    event_info             			   string         comment    '事件内容', 
    event_time             		       string         comment    '事件时间',   
    create_time            	           string         comment    '创建时间', 
    attr1          					   string         comment    '',    
    attr2             		           string         comment    '', 
    attr3               	           string      comment    ''
	)
	
PARTITIONED BY (`pt` string)
ROW FORMAT SERDE 
	  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
WITH SERDEPROPERTIES ( 
	  'field.delim'='\t', 
	  'serialization.format'='\t') 
STORED AS INPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'

