DROP TABLE IF EXISTS marketing_modeling.app_failed_reason;
CREATE TABLE IF NOT EXISTS `marketing_modeling.app_failed_reason`(
	  `mobile` string COMMENT "电话", 
	  `fail_reason_fir_level` array<string> COMMENT "一级战败原因",
	  `fail_reason_sec_level` array<string> COMMENT "二级战败原因"
)
--PARTITIONED BY(`pt`  string)
STORED AS TEXTFILE;