DROP TABLE IF EXISTS marketing_modeling.outbound_model_result;
CREATE TABLE IF NOT EXISTS marketing_modeling.outbound_model_result(
    `mobile` string comment '手机号', 
    `pred_score` double comment '战败下发模型打分', 
	 `score_rank` int comment '战败下发模型打分排序',
    `d_last_dealfail_d` timestamp comment '最后一次战败时间', 
    `c_province` string comment '省份', 
    `c_city` string comment '城市', 
    `c_sex` string comment '性别', 
    `c_age` string comment '年龄',
    `second_resource_name` string comment '最后一次二级线索来源'
   
)
PARTITIONED BY (pt STRING COMMENT 'datetime');