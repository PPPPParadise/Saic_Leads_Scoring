#!/usr/bin/env python
# coding: utf-8

'''
Training Pipeline of MG Failed Outbound Model
Created on Mon Oct 26 2020
@author: Xiaofeng XU
'''

import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
from datetime import datetime
import logging
import sys
import yaml

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType


###########################################
#           Set up Logs          #
##########################################
logger = logging.getLogger("Feature_Engineering")
logger.setLevel(logging.DEBUG) 

fh = logging.FileHandler("Train_MG_fail_outbound_model.log")
fh.setLevel(logging.INFO) 
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)


###########################################
#     Load configs and get paths     #
##########################################
with open('config.yaml') as config_file:
    config = yaml.full_load(config_file)

pt_date = sys.argv[1]

cols_used = pd.read_csv(config['cols_used']).cols_used # columns used to train model
ct_lv = pd.read_csv(config['input_data']['city_level'], sep='\t', encoding='utf-8') # city level mapping data
logger.info('Load Configs - Loaded success!')


##################################
#        Fetch Data       #
#################################
spark_session = SparkSession.builder.enableHiveSupport().appName("M_Model").config("spark.driver.memory","10g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)

hc.setConf("tez.queue.name","malg")
hc.setConf("mapreduce.job.queuename", "malg")
outbound_features = hc.sql('''
    SELECT * FROM marketing_modeling.app_sis_model_features
    WHERE 
        mobile REGEXP "^[1][3-9][0-9]{9}$"
        AND pt = "%s"
'''%(pt_date)).toPandas()

print(outbound_features.columns)
print(outbound_features.head(5))

# Merge data
outbound_features['c_city_level'] = outbound_features.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['city_level']
outbound_features['c_province'] = outbound_features.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['province']

logger.info('Fetch Data - Loaded success!')


##################################
#     Feature Engineering    #
#################################
# deal with datetime features
def calculate_datediff(col_1,col_2,output_col):
    for col in [col_1,col_2]:
        outbound_features[col] = pd.to_datetime(outbound_features[col])
    outbound_features[output_col] = (outbound_features[col_1] - outbound_features[col_2]).apply(lambda x: x.days)
    return outbound_features[output_col]

def string_to_list_1(x):
    '''
    Ignore multiple cities & provinces
    x(string): input string-type list
    '''
    try:
        if '|' in x:
            return np.nan
        elif len(x) == 0:
            return np.nan
        else:
            return x
    except:
        return np.nan

def age_process(x):
    if x == u'20岁以下':
        return 0
    elif x == u'21-25岁':
        return 1
    elif x == u'26-30岁':
        return 2
    elif x == u'31-35岁':
        return 3
    elif x == u'36-40岁':
        return 4
    elif x == u'41-45岁':
        return 5
    elif x== u'46-50岁':
        return 6
    elif x == u'51-55岁':
        return 7
    elif x == u'56-60岁':
        return 8
    elif x == u'60岁以上':
        return 9

def city_process(x):
    if x == u'一线城市':
        return 0
    elif x == u'新一线城市':
        return 1
    elif x == u'二线城市':
        return 2
    elif x == u'三线城市':
        return 3
    elif x == u'四线城市':
        return 4
    elif x == u'五线城市':
        return 5

def fir_source_process(x):
    if x == u'SIS接口分配':
        return 1.0
    elif x == u'厂方导入':
        return 2.0
    elif x == u'厂方的网销其他平台':
        return 3.0
    elif x == u'广告投放':
        return 4.0
    elif x == u'第一方触点':
        return 5.0
    elif x == u'经销商网销主动开拓':
        return 6.0
    elif x == u'网站平台接口':
        return 7.0
    
def sec_source_process(x):
    if x == u'汽车之家':
        return 1.0
    elif x == u'易车网':
        return 2.0
    elif x == u'懂车帝':
        return 3.0
    elif x == u'null':
        return 5.0
    else:
        return 4.0

# deal with datetime features
outbound_features['d_fircard_dealf_diff'] = calculate_datediff('last_fail_date','fir_card_date','d_fircard_dealf_diff')
outbound_features['d_fircard_firdealf_diff'] = calculate_datediff('fir_fail_date','fir_card_date','d_fircard_firdealf_diff')
outbound_features['d_dealf_lastvisit_diff'] = calculate_datediff('last_fail_date','last_visit_date','d_dealf_lastvisit_diff')
outbound_features['fircard_ob_diff'] = calculate_datediff('call_time','fir_card_date','fircard_ob_diff')
outbound_features['lastfollow_ob_diff'] = calculate_datediff('call_time','last_leads_date','lastfollow_ob_diff')
outbound_features['fail_ob_diff'] = calculate_datediff('call_time','last_fail_date','fail_ob_diff')
logger.info('Feature Engineering - Datediff features processing success!')

# deal with categorical features
## demographical features 
outbound_features['c_age'] = outbound_features['c_age'].apply(string_to_list_1)
outbound_features['c_city'] = outbound_features['c_city'].apply(string_to_list_1)

# deal with categorical features
## demographical features 
outbound_features['c_age'] = outbound_features['c_age'].apply(lambda x:age_process(x))
outbound_features['c_city_level'] = outbound_features['c_city_level'].apply(lambda x:city_process(x))
## leads source features
outbound_features['m_last_lead_first_source'] = outbound_features['m_last_lead_first_source'].apply(lambda x:fir_source_process(x))
outbound_features = outbound_features.join(pd.get_dummies(outbound_features['m_last_lead_first_source'],prefix="R1",prefix_sep="_"),on = None)
outbound_features['m_last_lead_second_source'] = outbound_features['m_last_lead_second_source'].apply(lambda x:sec_source_process(x))
outbound_features = outbound_features.join(pd.get_dummies(outbound_features['m_last_lead_second_source'],prefix="R2",prefix_sep="_"))

## customer graded level
outbound_features['lowest_cust_level'] = [np.nan if i is None else int(i[-1]) for i in outbound_features.cust_level]

## hostorical outbound performance
outbound_features["call_month"] = pd.to_datetime(outbound_features["call_time"]).dt.month
outbound_features["outbound_vol"] = outbound_features["failed_called"].map({0:1,1:0})

## number of null features
null_col = ['c_city_level','c_sex','c_age','visit_count','trial_count','activities_count']
outbound_features['null_vol'] = outbound_features[null_col].isna().sum(axis=1)

logger.info('Feature Engineering - Categorical features processing success!')

columns_map = {
'dealer_count':'d_leads_dtbt_count'
,'leads_model_count':'d_leads_car_model_count'
,'leads_count':'d_leads_count'
,'fail_count':'d_deal_fail_times'
,'visit_count':'d_visit_ttl'
,'card_count':'d_card_ttl'
,'leads_source_count':'count_of_resource'   
}
outbound_features = outbound_features.rename(columns = columns_map)
logger.info('Feature Engineering - Column rename success!')

print(outbound_features.columns)

##################################
#        Save Data       #
#################################
outbound_features[['mobile','ob_result','brand'] + list(cols_used)].to_csv('Training_Data/outbound_features.csv',index=False)
logger.info('Feature Engineering - Features saved!')
