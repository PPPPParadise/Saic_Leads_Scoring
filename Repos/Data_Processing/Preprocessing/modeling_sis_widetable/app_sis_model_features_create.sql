DROP TABLE IF EXISTS marketing_modeling.app_sis_model_features;
CREATE TABLE IF NOT EXISTS marketing_modeling.app_sis_model_features (
  `mobile` string  COMMENT '手机',
  `brand` string COMMENT '品牌 MG、RW',
  `call_time` string COMMENT '外呼日期',
  `ob_result` string COMMENT '外呼结果：是否下发',
  `fir_fail_date` string COMMENT '第一次战败日期',
  `last_fail_date` string COMMENT '最后一次战败日期（截至呼叫时间当天）',
  `fir_card_date` string COMMENT '第一次建卡日期',
  `last_card_date` string COMMENT '最后一次建卡日期（截至呼叫时间当天）',
  `last_leads_date` string COMMENT '最后一次留资日期（截至呼叫时间当天）',
  `last_visit_date` string COMMENT '最后一次到店日期（截至呼叫时间当天）',
  `fail_count` string COMMENT '战败次数（截至呼叫时间当天）',
  `leads_count` string COMMENT '总留资次数（截至呼叫时间当天）',
  `card_count` string COMMENT '总建卡次数（截至呼叫时间当天）',
  `visit_count` string COMMENT '总到店次数（截至呼叫时间当天）',
  `activities_count` string COMMENT '总参加活动次数（截至呼叫时间当天）',
  `trial_count` string COMMENT '总试乘试驾次数（截至呼叫时间当天）',
  `dealer_count` string COMMENT '留资经销商数量（截至呼叫时间当天）',
  `leads_model_count` string  COMMENT '意向车型数量（截至呼叫时间当天）',
  `failed_called` string COMMENT '历史是否被战败外呼（截至呼叫时间当天）',
  `c_age` string COMMENT '年龄',
  `c_city` string COMMENT '城市',
  `c_sex` string COMMENT '性别',
  `leads_source_count` string COMMENT '一二级线索来源数量（截至呼叫时间当天）',
  `m_last_lead_first_source` string COMMENT '最后一次一级线索来源（截至呼叫时间当天）',
  `m_last_lead_second_source` string COMMENT '最后一次二级线索来源（截至呼叫时间当天）',
  `cust_level` string COMMENT '历史最低销售代表评定意向等级'
  
 )
PARTITIONED BY (pt STRING COMMENT "datetime")
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  ;


