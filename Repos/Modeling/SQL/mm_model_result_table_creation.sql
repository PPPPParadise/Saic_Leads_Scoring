DROP TABLE IF EXISTS marketing_modeling.app_model_result;
CREATE TABLE IF NOT EXISTS marketing_modeling.app_model_result(
mobile 				string				comment			'手机号',
pred_score			float				comment			'分数',
result_date			timestamp   		comment			'更新日期'
)
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS ORCFILE;