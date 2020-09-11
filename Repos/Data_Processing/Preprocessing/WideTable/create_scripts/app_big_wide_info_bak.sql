DROP TABLE IF EXISTS marketing_modeling.app_big_wide_info_bak;
CREATE TABLE marketing_modeling.app_big_wide_info_bak(                  
   mobile string COMMENT '用户唯一ID，mobile',         
   d_fir_leads_time timestamp COMMENT '首次留资时间',   
   d_last_leads_time timestamp COMMENT '最后留资时间',  
   d_fir_sec_leads_diff int COMMENT '首次留资时间与二次留资时间日期差值',  
   d_leads_count bigint COMMENT '总留资次数',          
   d_leads_channel array<int> COMMENT '留资渠道',     
   d_avg_leads_date double COMMENT '平均留资时间间隔',    
   d_leads_channel_count bigint COMMENT '留资渠道总数',  
   d_leads_dtbt_count bigint COMMENT '留资总经销商数',   
   d_leads_dtbt_coincide double COMMENT '留资经销商重合度',  
   d_leads_dtbt_level_1 bigint COMMENT '一级经销商留资数量',  
   d_leads_dtbt_level_2 bigint COMMENT '二级经销商留资数量',  
   d_cust_type int COMMENT '客户属性 (个人或企业)',        
   d_fir_card_time timestamp COMMENT '首次建卡时间',    
   d_last_card_time timestamp COMMENT '首次建卡时间',   
   d_card_ttl bigint COMMENT '总建卡次数',             
   d_cust_ids array<bigint> COMMENT '建卡ID数组',     
   d_clue_issued_times bigint COMMENT '线索总下发次数',  
   d_fir_visit_time timestamp COMMENT '首次到店时间',   
   d_fir_sec_visit_diff int COMMENT '首次到店时间与二次到店时间日期差值',  
   d_visit_dtbt_count bigint COMMENT '到店总经销商数量',  
   d_visit_ttl bigint COMMENT '总到店次数',            
   d_avg_visit_date double COMMENT '平均到店时间间隔',    
   d_dealer_ids array<bigint> COMMENT ' 经销商ID数组',  
   d_avg_visit_dtbt_count double COMMENT '平均每个经销商处到店次数',  
   d_followup_ttl bigint COMMENT '总跟进次数',         
   d_fir_trail timestamp COMMENT '-首次试乘试驾时间',     
   d_last_reservation_time timestamp COMMENT ' 最后一次试驾预约试驾',  
   d_trail_book_tll bigint COMMENT '-试乘试驾总预定次数',  
   d_trail_attend_ttl bigint COMMENT '-试乘试驾总参与次数',  
   d_trail_attend_ppt double COMMENT '-试乘试驾参与率',  
   d_leads_car_model_count bigint COMMENT '留资车型数量',  
   d_leads_car_model_type bigint COMMENT '留资车型种类数',  
   d_vel_series_ids array<bigint> COMMENT '车型IDs',  
   d_series_types array<string> COMMENT '车型种类IDs',  
   d_rw_car_model_count bigint COMMENT '荣威留资车型数量',  
   d_deal_time timestamp COMMENT ' 首次成交时间',       
   d_last_deal_time timestamp COMMENT ' 最后成交时间',  
   d_deal_ttl bigint COMMENT ' 总成交次数',            
   d_deal_car_ttl bigint COMMENT ' 两年内总购买车辆数',    
   d_fir_dealfail_d timestamp COMMENT '- 首次战败时间',  
   d_last_dealfail_d timestamp COMMENT ' 最后战败时间',  
   d_is_deposit_order int COMMENT '-是否下定',        
   d_deal_flag int COMMENT '- 是否成交 1-成交，0-战败，null-跟进',  
   d_leads_pool_status string COMMENT '状态：001-未下发；002-培育；003-跟进',  
   d_dealf_succ_firvisit_diff double COMMENT '成交时间与成交经销商处首次到店时间日期差值',  
   d_dealf_succ_lastvisit_diff double COMMENT '成交时间与成交经销商处最后一次到店时间日期差值',  
   d_avg_fir_sec_visit_diff double COMMENT '平均首次到店时间与二次到店时间日期差值',  
   d_fircard_firvisit_dtbt_diff array<int> COMMENT '建卡时间距首次到店时间日期差值(每个经销商有个value)',  
   d_avg_fircard_firvisit_diff double COMMENT '平均建卡时间距首次到店时间日期差值',  
   d_avg_firleads_firvisit_diff double COMMENT '平均首次留资时间距首次到店时间日期差值',  
   d_fir_activity_time timestamp COMMENT '首次活动时间',  
   d_activity_ttl bigint COMMENT '总活动次数',         
   d_natural_visit string COMMENT '自然到店',         
   d_fir_leads_deal_diff_y double COMMENT '上次成交时间距首次留资时间年数',  
   d_has_deliver_history int COMMENT '之前是否荣威/名爵车主',  
   d_trail_count_d30 bigint COMMENT '去30天总试驾数',  
   d_trail_count_d90 bigint COMMENT '过去30天总试驾数',  
   d_visit_count_d15 bigint COMMENT '过去15天总到店次数',  
   d_visit_count_d30 bigint COMMENT '过去30天总到店次数',  
   d_visit_count_d90 bigint COMMENT '过去90天总到店次数',  
   d_followup_d7 bigint COMMENT '过去7天跟进次数',       
   d_followup_d15 bigint COMMENT '过去15天跟进次数',     
   d_followup_d30 bigint COMMENT '过去30天跟进次数',     
   d_followup_d60 bigint COMMENT '过去60天跟进次数',     
   d_followup_d90 bigint COMMENT '过去90天跟进次数',     
   d_activity_count_d15 bigint COMMENT '过去15天参加活动次数',  
   d_activity_count_d30 bigint COMMENT '过去30天参加活动次数',  
   d_activity_count_d60 bigint COMMENT '过去60天参加活动次数',  
   d_activity_count_d90 bigint COMMENT '过去90天参加活动次数',  
   d_firlead_dealf_diff int COMMENT '首次留资时间距最后成交/战败时间差值',  
   d_lastlead_dealf_diff int COMMENT '最后一次留资时间距最后成交/战败时间差值',  
   d_last_dfail_dealf_diff int COMMENT '客户最后一次战败日期距成交/战败日期天数',  
   d_firfollow_dealf_diff int COMMENT '首次跟进日期距成交/战败时间差值',  
   d_lastfollow_dealf_diff int COMMENT '最后一次跟进日期距成交/战败日期差值',  
   d_dealf_lastvisit_diff int COMMENT '成交/战败时间与全经销商处最后一次到店时间日期差值',  
   d_dealf_firvisit_diff int COMMENT '成交/战败时间与全经销商处首次到店时间日期差值',  
   d_lasttrail_dealf_diff int COMMENT '最后一次试乘试驾日期距最后成交/战败时间差值',  
   d_firleads_firvisit_diff int COMMENT '首次留资时间与首次到店时间日期差值',  
   d_leads_dtbt_ppt double COMMENT '留资经销商到店比例',   
   d_fircard_firvisit_diff int COMMENT '首次建卡时间距首次到店时间日期差值',  
   d_fir_dealfail_deal_diff int COMMENT '客户首次战败日期距成交时间天数',  
   d_deal_fail_ppt double COMMENT '战败比例',         
   d_last_activity_dealf_diff int COMMENT '最后一次参加活动时间距成交(战败)日期差值',  
   d_fir_activity_dealf_diff int COMMENT '首次参加活动时间距成交(战败)日期差值',  
   d_fir_order_leads_diff int COMMENT '首次下定时间与首次留资时间的日期差值',  
   d_fir_order_visit_diff int COMMENT '首次下定时间与首次到店时间的日期差值',  
   d_fir_order_trail_diff int COMMENT '首次下定时间与首次试乘试驾的日期差值',  
   c_is_app_user int COMMENT '是否注册APP',           
   c_brwose_count_d14 int COMMENT '过去14天浏览官网页面次数',  
   c_avg_browse_time_d14 int COMMENT '过去14天访问APP平均时长',  
   c_last_app_visit_time int COMMENT '最近一次访问APP时间',  
   c_app_point_ttl_d14 int COMMENT '过去14天取得积分总数',  
   c_app_point_ttl int COMMENT '总使用积分次数',         
   c_last_app_point_use_time int COMMENT '最近一次使用积分时间',  
   c_is_trail_user int COMMENT '是否试乘试驾',          
   c_trail_vel string COMMENT '试驾车系',             
   c_last_trail_vel string COMMENT '最近一次试驾车系',    
   c_deliver_vel string COMMENT '购买车系',
   c_visit_count int COMMENT '总到店次数',             
   c_visit_count_d14 int COMMENT '过去14天到店次数',     
   c_deal_time string COMMENT '成交日期',             
   c_last_trail_time int COMMENT '最近一次试驾日期',      
   c_last_lead_time int COMMENT '最近跟进日期',         
   c_trail_count_d14 int COMMENT '过去14天预约试乘试驾次数',  
   c_last_reach_platform string COMMENT '最近触达平台',  
   c_lead_sources string COMMENT '线索来源',          
   c_last_bbs_time int COMMENT '最近一次查看资讯论坛时间',    
   c_order_time string COMMENT 'APP下订日期',         
   c_last_sis_result string COMMENT '上次外呼结果',     
   c_open_link int COMMENT '是否打开短链',              
   c_last_visit_time string COMMENT '最近一次进店时间',   
   c_last_activity_time string COMMENT '最近参加活动日期',  
   c_create_card_count int COMMENT '跨店建卡数量',      
   c_cust_live_period int COMMENT '客户寿命(当前时间-客户最早留资时间)',  
   c_followup_ttl int COMMENT '历史跟进次数',           
   c_last_sis_time string COMMENT '上次外呼日期',       
   c_deal_fail_time string COMMENT '战败日期',        
   c_lead_model string COMMENT '留资车系',            
   c_fail_status int COMMENT '战败核实状态',            
   c_rx5_video_time int COMMENT 'RX5 MAX视频观看记录(APP端RX5 MAX视频观看类型)',  
   c_marvel_video_time int COMMENT 'MARVELX视频观看记录',  
   c_sex string COMMENT '性别',                     
   c_age string COMMENT '年龄',                     
   c_province string COMMENT '省份',                
   c_city string COMMENT '城市',                    
   c_last_lost_type int COMMENT '最近流失预警类型',       
   c_last_lost_time int COMMENT '最近流失预警日期',       
   c_roewe_brwose_homepage int COMMENT '是否浏览爱车展厅了解详情车型及配置页',  
   c_search_vel int COMMENT '是否搜索过车型关键词',         
   c_browse_model_info int COMMENT '是否观看过车型的资讯',  
   c_trail int COMMENT '是否预约过车型试驾',               
   c_make_deal int COMMENT '是否下订',                
   c_model int COMMENT '关注车系',                    
   c_register_time int COMMENT 'APP注册时间',         
   c_last_cust_base_time int COMMENT '最近一次建卡时间',  
   c_fail_real_result int COMMENT '战败核实结果',       
   c_app_browse_count_d14 int COMMENT '过去14天浏览APP次数',  
   h_goal string COMMENT '购车目的',                  
   h_loan int COMMENT '贷款概率',                     
   h_priority_car_type string COMMENT '(高优先级)购车喜好',  
   h_configuration_preference string,             
   h_priority_config_prefer string COMMENT '(高优先级)配置喜好,(动力偏好)',  
   h_priority_car_usage string COMMENT '购车用途',    
   h_client_focus string COMMENT '客户关注点',         
   h_models string COMMENT '车型偏好',                
   h_model_nums int COMMENT '关注竞品数量',             
   h_have_car int COMMENT '是否有车:1是,-1否,0空',       
   h_compete int COMMENT '本品竞争度:1低2中3高',          
   h_compete_car_num_d30 int COMMENT '30天关注竞品数量',  
   h_compete_car_num_d60 int COMMENT '60天关注竞品数量',  
   h_compete_car_num_d90 int COMMENT '90天关注竞品数量',  
   h_focusing_avg_diff int COMMENT '竞品关注度和名爵车型平均关注度的差值',  
   h_focusing_max_diff int COMMENT '竞品关注度和名爵车型最高关注度的差值',  
   h_func_prefer string COMMENT '功能偏好:functional_preference字段是否出现该词',  
   h_car_type_prefer string COMMENT '购车喜好',       
   h_config_prefer string COMMENT '单个竞品最大询价次数',   
   h_budget string COMMENT '购车预算',                
   h_displace int COMMENT '置换概率',                 
   h_level string COMMENT '级别',                   
   h_volume string COMMENT '排量',                  
   h_priority_change_car_and_buy int,             
   h_ttl_inquiry_time int COMMENT '所有竞品总询价次数',    
   h_max_inquiry_time int COMMENT '单个竞品车型最大询价次数',  
   h_produce_way string COMMENT '生产方式',           
   h_config_nums int COMMENT '配置标签数',             
   h_leads_create_time date COMMENT '留资时间',       
   s_firstleads_lastcard_call_count int COMMENT '首次留资时间和最后一次建卡时间之间的总外呼次数',  
   s_firstleads_firstcard_call_count int COMMENT '首次留资时间和首次建卡时间之间的总外呼次数',  
   s_avg_talk_length int COMMENT '平均通话时长（分钟）',    
   s_talk_connet_ppt float COMMENT '接通率',         
   s_first_call_result string COMMENT '首次通话结果',   
   d_fail_desc_list array<varchar(2000)> COMMENT '战败原因',  
   d_fail_time_list array<timestamp> COMMENT '战败时间',  
   d_cust_dealer_ids array<bigint> COMMENT '建卡经销商IDs',  
   m_last_visit_time timestamp COMMENT '最后一次到店日期',  
   m_focus_series_count int COMMENT '意向车型数量（来自oppor表）',  
   m_oppr_fail_count int COMMENT '战败次数',          
   m_cust_max_level string COMMENT '最大意向级别',      
   m_failed_called int COMMENT '是否被战败外呼',         
   m_lead_source_count int COMMENT '一二级线索来源数量',   
   m_last_lead_first_source string COMMENT '最后一次一级线索来源',  
   m_last_lead_second_source string COMMENT '最后一次二级线索来源') 
 PARTITIONED BY (                                   
   pt string COMMENT 'datetime')                  
 ROW FORMAT SERDE                                   
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  
 WITH SERDEPROPERTIES (                             
   'collection.delim'=',',                          
   'field.delim'='\t',                              
   'mapkey.delim'=':',                              
   'serialization.format'='\t')                     
 STORED AS INPUTFORMAT                              
   'org.apache.hadoop.mapred.TextInputFormat'       
 OUTPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
;

set hive.exec.dynamic.partition =true;
set hive.exec.dynamic.partition.mode = nonstrict;
INSERT INTO  marketing_modeling.app_big_wide_info_bak
SELECT
	mobile
,d_fir_leads_time
,d_last_leads_time
,d_fir_sec_leads_diff
,d_leads_count
,d_leads_channel
,d_avg_leads_date
,d_leads_channel_count
,d_leads_dtbt_count
,d_leads_dtbt_coincide
,d_leads_dtbt_level_1
,d_leads_dtbt_level_2
,d_cust_type
,d_fir_card_time
,d_last_card_time
,d_card_ttl
,d_cust_ids
,d_clue_issued_times
,d_fir_visit_time
,d_fir_sec_visit_diff
,d_visit_dtbt_count
,d_visit_ttl
,d_avg_visit_date
,d_dealer_ids
,d_avg_visit_dtbt_count
,d_followup_ttl
,d_fir_trail
,d_last_reservation_time
,d_trail_book_tll
,d_trail_attend_ttl
,d_trail_attend_ppt
,d_leads_car_model_count
,d_leads_car_model_type
,d_vel_series_ids
,d_series_types
,d_rw_car_model_count
,d_deal_time
,d_last_deal_time
,d_deal_ttl
,d_deal_car_ttl
,d_fir_dealfail_d
,d_last_dealfail_d
,d_is_deposit_order
,d_deal_flag
,d_leads_pool_status
,d_dealf_succ_firvisit_diff
,d_dealf_succ_lastvisit_diff
,d_avg_fir_sec_visit_diff
,d_fircard_firvisit_dtbt_diff
,d_avg_fircard_firvisit_diff
,d_avg_firleads_firvisit_diff
,d_fir_activity_time
,d_activity_ttl
,d_natural_visit
,d_fir_leads_deal_diff_y
,d_has_deliver_history
,d_trail_count_d30
,d_trail_count_d90
,d_visit_count_d15
,d_visit_count_d30
,d_visit_count_d90
,d_followup_d7
,d_followup_d15
,d_followup_d30
,d_followup_d60
,d_followup_d90
,d_activity_count_d15
,d_activity_count_d30
,d_activity_count_d60
,d_activity_count_d90
,d_firlead_dealf_diff
,d_lastlead_dealf_diff
,d_last_dfail_dealf_diff
,d_firfollow_dealf_diff
,d_lastfollow_dealf_diff
,d_dealf_lastvisit_diff
,d_dealf_firvisit_diff
,d_lasttrail_dealf_diff
,d_firleads_firvisit_diff
,d_leads_dtbt_ppt
,d_fircard_firvisit_diff
,d_fir_dealfail_deal_diff
,d_deal_fail_ppt
,d_last_activity_dealf_diff
,d_fir_activity_dealf_diff
,d_fir_order_leads_diff
,d_fir_order_visit_diff
,d_fir_order_trail_diff
,c_is_app_user
,c_brwose_count_d14
,c_avg_browse_time_d14
,c_last_app_visit_time
,c_app_point_ttl_d14
,c_app_point_ttl
,c_last_app_point_use_time
,c_is_trail_user
,c_trail_vel
,c_last_trail_vel
,c_deliver_vel
,c_visit_count
,c_visit_count_d14
,c_deal_time
,c_last_trail_time
,c_last_lead_time
,c_trail_count_d14
,c_last_reach_platform
,c_lead_sources
,c_last_bbs_time
,c_order_time
,c_last_sis_result
,c_open_link
,c_last_visit_time
,c_last_activity_time
,c_create_card_count
,c_cust_live_period
,c_followup_ttl
,c_last_sis_time
,c_deal_fail_time
,c_lead_model
,c_fail_status
,c_rx5_video_time
,c_marvel_video_time
,c_sex
,c_age
,c_province
,c_city
,c_last_lost_type
,c_last_lost_time
,c_roewe_brwose_homepage
,c_search_vel
,c_browse_model_info
,c_trail
,c_make_deal
,c_model
,c_register_time
,c_last_cust_base_time
,c_fail_real_result
,c_app_browse_count_d14
,h_goal
,h_loan
,h_priority_car_type
,h_configuration_preference
,h_priority_config_prefer
,h_priority_car_usage
,h_client_focus
,h_models
,h_model_nums
,h_have_car
,h_compete
,h_compete_car_num_d30
,h_compete_car_num_d60
,h_compete_car_num_d90
,h_focusing_avg_diff
,h_focusing_max_diff
,h_func_prefer
,h_car_type_prefer
,h_config_prefer
,h_budget
,h_displace
,h_level
,h_volume
,h_priority_change_car_and_buy
,h_ttl_inquiry_time
,h_max_inquiry_time
,h_produce_way
,h_config_nums
,h_leads_create_time
,s_firstleads_lastcard_call_count
,s_firstleads_firstcard_call_count
,s_avg_talk_length
,s_talk_connet_ppt
,s_first_call_result
,d_fail_desc_list
,d_fail_time_list
,d_cust_dealer_ids
,m_last_visit_time
,m_focus_series_count
,m_oppr_fail_count
,m_cust_max_level
,m_failed_called
,m_lead_source_count
,m_last_lead_first_source
,m_last_lead_second_source
,pt
FROM
	 marketing_modeling.app_big_wide_info
WHERE pt >='20200828'
;

