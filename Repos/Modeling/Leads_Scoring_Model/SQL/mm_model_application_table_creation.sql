CREATE TABLE IF NOT EXISTS marketing_modeling.app_model_application(

mobile 				string				comment			'手机号',
outbound			int					comment			'是否外呼',
prob_tag			string   			comment			'意向等级'
)
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS ORCFILE;