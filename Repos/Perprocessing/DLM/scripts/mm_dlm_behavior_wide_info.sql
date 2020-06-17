
DROP TABLE IF EXISTS marketing_modeling.mm_dlm_behavior_wide_info;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.mm_dlm_behavior_wide_info (
    mobile                             bigint      comment    '用户唯一ID，phone num',
    deal                               int         comment    '成交与否',
    fir_leads_time                     TIMESTAMP    comment   '首次留资时间',     
    firlead_dealf_diff                 int         comment    '首次留资时间距成交(战败)时间差值',
    lastlead_dealf_diff                int         comment    '最后一次留资时间距成交(战败)时间差值', 
    fir_sec_leads_diff                 int         comment    '首次留资时间与二次留资时间日期差值', 
    avg_leads_date                     float       comment    '平均留资时间间隔',   
    leads_count                        int         comment    '总留资次数', 
    leads_channel                      string      comment    '留资渠道',    
    leads_channel_count                int         comment    '留资渠道总数', 
    leads_dtbt_count                   int         comment    '留资总经销商数', 
    leads_dtbt_coincide                float       comment    '留资经销商重合度',   
    leads_dtbt_level                   float       comment    '留资经销商平均等级',   
    leads_car_model_count              int         comment    '留资车型数量',      
    leads_car_model_type               int         comment    '留资车型种类数',
    rw_car_mode_count                  int         comment    '荣威留资车型数量',
    clue_issued_times                  int         comment    '线索总下发次数',

    fir_card_time                      TIMESTAMP    comment     '首次建卡时间',
    fircard_dealf_diff                 int         comment     '首次建卡时间距成交(战败)时间差值',
    fircard_firvisit_dtbt_diff         ARRAY<int>         comment     '建卡时间距首次到店时间日期差值(每个经销商有个value)',
    fir_visit_time                     TIMESTAMP   comment     '首次到店时间',     
    fir_sec_visit_diff                 int         comment     '首次到店时间与二次到店时间日期差值',
    fir_sec_avgvisit_diff              float       comment     '平均首次到店时间与二次到店时间日期差值,客户在每个经销商下首次到店时间与二次到店时间日期差值的平均值，若该客户在该经销商下未二次到店，则不考虑该经销商',  
    avg_visit_date                     int         comment     '平均到店时间间隔',
    firleads_firvisit_diff             int         comment     '首次留资时间与首次到店时间日期差值',
    avg_firleads_firvisit_diff         float       comment     '平均首次留资时间距首次到店时间日期差值',  
    fircard_firvisit_diff              int         comment     '首次建卡时间距首次到店时间日期差值',
    avg_fircard_firvisit_diff          float       comment     '平均建卡时间距首次到店时间日期差值',  

    visit_dtbt_count                   int         comment     '到店总经销商数量',
    visit_ttl                          int         comment     '总到店次数',
    avg_visit_dtbt_count               float       comment     '平均每个经销商处到店次数',  
    leads_dtbt_ppt                     float       comment     '留资经销商到店比例',  
    visit_count_d15                    int         comment     '过去15天总到店次数',
    visit_count_d30                    int         comment     '过去30天总到店次数',
    visit_count_d90                    int         comment     '过去90天总到店次数',
    trail_book_tll                     int         comment     '试乘试驾总预定次数',
    trail_attend_ttl                   int         comment     '试乘试驾总参与次数',
    trail_attend_ppt                   float       comment     '试乘试驾参与率',  
    trail_count_d30                    int         comment     '过去30天试乘试驾次数',
    trail_count_d90                    int         comment     '过去90天试乘试驾次数',
    fir_trail                          TIMESTAMP   comment     '首次试乘试驾时间',     
    lasttrail_deald_diff               int         comment     '最后一次试乘试驾日期距成交(战败)日期天数',

    followup_ttl                       int         comment     '总跟进次数',
    followup_d7                        int         comment     '过去7天跟进次数',
    followup_d15                       int         comment     '过去15天跟进次数',
    followup_d30                       int         comment     '过去30天跟进次数',
    followup_d60                       int         comment     '过去60天跟进次数',
    followup_d90                       int         comment     '过去90天跟进次数',
    lastfollow_dealf_diff              int         comment     '最后一次跟进日期距成交(战败)日期差值',
    firfollow_dealf_diff               int         comment     '首次跟进日期距成交(战败)时间差值',
         
    deal_time                          TIMESTAMP    comment     '成交时间', 
    dealf_firvisit_diff                int         comment     '成交(战败)时间与全经销商处首次到店时间日期差值',  
    dealf_succ_firvisit_diff           int         comment     '成交(战败)时间与成交经销商处首次到店时间日期差值',  
    dealf_lastvisit_diff               int         comment     '成交(战败)时间与全经销商处最后一次到店时间日期差值',  
    dealf_succ_lastvisit_diff          int         comment     '成交(战败)时间与成交经销商处最后一次到店时间日期差值',  
    deal_last_trail_diff               int         comment     '成交时间与最后一次试乘试驾日期差值',  
    deal_fail_times                    int         comment     '战败次数',  
    deal_fail_ppt                      float       comment     '战败比例',    
    last_dfail_dealf_diff              int         comment     '客户最后一次战败日期距成交(最终战败)日期天数',  
    fir_dealfail_d                     int         comment     '客户首次战败日期天数',  
    fir_dealfail_deal_diff             int         comment     '客户首次战败日期距成交时间天数'
)
PARTITIONED BY (dt STRING COMMENT "DATE", hour STRING COMMENT "hour")
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE LOCATION 'hdfs://tcluster/user/hive/marketing_modeling/dlm/mm_dlm_behavior_wide_info';
