DROP TABLE IF EXISTS marketing_modeling.app_mkt_auto_script;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_mkt_auto_script (
    id                            	   bigint      comment    '用户唯一ID，自增',
    mobile              	           string      comment    '手机号',
    dealer_id           	           string      comment    '经销商id',
    auto_script_id            		   string      comment    '事件id', 
    create_time            	           date        comment    '创建时间', 
	orders            	   		       int         comment    '排序', 
    attr1          					   string      comment    '',    
    attr2             		           string      comment    '', 
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

