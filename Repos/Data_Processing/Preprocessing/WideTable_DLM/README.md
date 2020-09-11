### 1. 结构
```SQL
├──README.md
├──load_ods_data                   #加载ods的各种数据
    |──load_dlm_t_leads_m          #留资数据
        |──leads_feature_cleansing.sql    #数据清洗
		|──leads_feature_processing.sql   #数据处理
		|──run.sh
    |──load_dlm_t_cust_base        #建卡数据
        |──cust_feature_cleansing.sql     #数据清洗
		|──cust_feature_processing.sql    #数据处理
		|──run.sh
    |──load_dlm_t_followup         #跟进数据
        |──followup_feature_cleansing.sql   #数据清洗
		|──followup_feature_processing.sql   #数据处理
		|──run.sh
    |──load_dlm_t_oppor            #意向数据
        |──oppor_feature_cleansing.sql      #数据清洗
		|──oppor_feature_processing.sql     #数据处理
		|──run.sh
    |──load_dlm_t_oppor_fail       #战败数据
        |──fail_feature_cleansing.sql       #数据清洗
		|──fail_feature_processing.sql      #数据处理
		|──run.sh
    |──load_dlm_t_deliver_vel      #战胜数据
        |──deliver_feature_cleansing.sql     #数据清洗
		|──deliver_feature_processing.sql    #数据处理
		|──run.sh
    |──load_dlm_t_hall_flow        #到店数据
        |──hall_flow_feature_cleansing.sql   #数据清洗
		|──hall_flow_feature_processing.sql  #数据处理
		|──run.sh
    |──load_dlm_t_trial_receive     #试驾数据
        |──trial_receive_feature_cleansing.sql   #数据清洗
		|──trial_receive_feature_processing.sql  #数据处理
		|──run.sh
    |──load_dlm_t_cust_vehicle      #购车数据
        |──cust_vehicle_feature_cleansing.sql    #数据清洗
		|──run.sh
    |──load_dlm_t_cust_activity     #活动数据
        |──cust_activity_cleansing.sql        #数据清洗
		|──cust_activity_processing.sql       #数据处理
		|──run.sh
├──processing_feature_step1
    |──deal_flag_feature.sql                #计算用户成交战败特征
	|──mobile_mapping_processing.sql        #合并留资、建卡、到店用户，供后续特征映射mobile
	|──tmp_dlm_last_deal_time.sql           #最后留资时间
	|──tmp_dlm_feature_join_1.sql           #join所有预处理的特征
	|──run.sh
├──processing_feature_step2
    |──avg_fir_sec_visit_diff.sql               #平均首次到店时间与二次到店时间日期差值
	|──avg_fircard_firvisit_diff.sql            #平均建卡时间距首次到店时间日期差值
	|──avg_firleads_firvisit_diff.sql           #平均首次留资时间距首次到店时间日期差值
	|──dealf_succ_firvisit_diff.sql             #成交时间与成交经销商处首次到店时间日期差值
	|──dealf_succ_lastvisit_diff.sql            #成交时间与成交经销商处最后一次到店时间日期差值
	|──has_deliver_history.sql                  #是否荣威/名爵车主
	|──run.sh
	|──run_hdh.sh
├──processing_feature_step3
    |──activity_count_d15.sql                   #过去15天参加活动次数
	|──activity_count_d30.sql
	|──activity_count_d60.sql
	|──activity_count_d90.sql
	|──followup_d7.sql                          #过去7天跟进次数
    |──followup_d15.sql
	|──followup_d30.sql
	|──followup_d60.sql
	|──followup_d90.sql                         #过去90天跟进次数
	|──diff_dealf_firvisit.sql                  #成交时间与全经销商处首次到店时间日期差值
    |──diff_fircard_dealf.sql                   #首次建卡时间距最后成交时间差值
	|──diff_firfollow_dealf.sql                 #首次跟进日期距成交时间差值
	|──diff_firstlead_dealf.sql                 #首次留资时间距最后成交/战败时间差值
	|──diff_last_dfail_dealf.sql                #客户最后一次战败日期距成交日期天数
	|──diff_lastlead_dealf.sql                  #最后一次留资时间距最后成交时间差值
    |──diff_lasttrail_dealf.sql                 #最后一次试乘试驾日期距最后成交时间差值
	|──fir_activity_dealf_diff.sql              #首参加活动时间距成交(战败)日期差值
	|──fir_lead_dealf_diff_y.sql                #上次成交时间距首次留资时间年数
	|──fir_order_leads_diff.sql                 #首次下定时间与首次留资时间的日期差值
	|──fir_order_trail_diff.sql                 #首次下定时间与首次到店时间的日期差值
	|──fir_order_visit_diff.sql                 #首次下定时间与首次试乘试驾的日期差值
	|──join_some_diff_feature.sql               #战败比例、首次留资时间与首次到店时间日期差值、留资经销商到店比例、首次建卡时间距首次到店时间日期差值
	|──last_activity_dealf_diff.sql             #最后一次参加活动时间距成交(战败)日期差值
	|──natural_visit.sql                        #自然到店  到店时间早于留资时间
	|──trail_count_d30.sql                      #过去30天总试驾数
	|──trail_count_d90.sql                      #过去90天总试驾数
	|──visit_count_d15.sql                      #过去15天总到店数
	|──visit_count_d30.sql                      #过去30天总到店数
	|──visit_count_d90.sql                      #过去90天总到店数
	|──run1.sh            #包括时间差的特征脚本
	|──run2.sh            #包括N天的特征脚本
├──Aggregate_Feature
    |──tmp_dlm_feature_join_2.sql               #join step2中的特征
	|──tmp_dlm_feature_join_3.sql               #join step3中的N天类的特征
	|──tmp_dlm_feature_join_4.sql               #join step3中的其他时间差值特征
	|──tmp_leads_pool_status.sql                #用户线索池状态特征
	|──app_dlm_wide_info.sql                    #DLM小宽表
	|──run.sh
	|──run_status.sh
 
