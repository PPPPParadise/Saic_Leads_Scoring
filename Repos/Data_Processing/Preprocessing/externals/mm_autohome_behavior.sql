DROP TABLE IF EXISTS marketing_modeling.mm_autohome_user_behavior;
CREATE TABLE IF NOT EXISTS marketing_modeling.mm_autohome_user_behavior (
    cust_id                            bigint      comment    '电话号码',
    goal                               string      comment    '购车目的',
    loan                               int         comment    '贷款概率',     
    priority_car_type                  string      comment    '(高优先级)购车喜好',
    priority_config_prefer             string      comment    '(高优先级)配置喜好,(动力偏好)', 
    priority_car_usage                 string      comment    '购车用途', 
    client_focus                       string      comment    '客户关注点',   
    models                             string      comment    '车型偏好', 
    model_nums                         int         comment    '关注竞品数量',    
    have_car                           int         comment    '是否有车:1是,-1否,0空', 
    compete                            int         comment    '本品竞争度:1低2中3高', 
    compete_car_num_d30                int         comment    '30天关注竞品数量',   
    compete_car_num_d60                int         comment    '60天关注竞品数量',   
    compete_car_num_d90                int         comment    '90天关注竞品数量',      
    focusing_avg_diff                  int         comment    '竞品关注度和名爵车型平均关注度的差值',
    focusing_max_diff                  int         comment    '竞品关注度和名爵车型最高关注度的差值',
    config_prefer                      int         comment    '配置喜好'
    func_prefer                        int         comment    '功能偏好:functional_preference字段是否出现该词',
    query_price_ttl                    int         comment    '竞品询价总次数',
    query_price_max                    int         comment    '单个竞品最大询价次数',
    car_type                           int         comment    '购车喜好',     
    budget                             int         comment    '购车预算',
    displace                           int         comment    '置换概率',  
    config_nums                        int         comment    '配置标签数',
    level                              int         comment    '级别',
    volume                             int         comment    '排量',
    priority_change_car_and_buy        int         comment    '换车新购',
    ttl_inquiry_time                   int         comment    '所有竞品总询价次数',
    max_inquiry_time                   int         comment    '单个竞品车型最大询价次数',
    produce_way                        string      comment    '生产方式'


PARTITIONED BY (ts STRING COMMENT "timestamp")
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE LOCATION 'hdfs://tcluster/user/hive/marketing_modeling/autohome/mm_autohome_user_behavior';



