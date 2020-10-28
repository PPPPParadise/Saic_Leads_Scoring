#!/usr/bin/env python
# coding: utf-8

'''
MG Failed Outbound Model
Created on Fri Aug 31 2020
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
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType

###########################################
#           Set up Logs          #
##########################################
logger = logging.getLogger("Outbound_List_Generation")
logger.setLevel(logging.DEBUG) 

fh = logging.FileHandler("MG_fail_outbound_model.log")
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
outbound_date = sys.argv[2]
outbound_date = datetime.strptime(outbound_date,'%Y%m%d')

def find_newest_model(path,name):
    files = os.listdir(path)
    list_model = [i for i in files if i.find(name) != -1]
    list_model.sort(key=lambda x:os.path.getmtime(path+x))
    model = os.path.join(path,list_model[-1])
    return model

ct_lv = pd.read_csv(config['input_data']['city_level'], sep = '\t',encoding = 'utf-8')
model_1_path = find_newest_model('Model_File/','xgb_model1')
model_2_path = find_newest_model('Model_File/','xgb_model2')

logger.info('Loaded Model-1:%s',model_1_path)
logger.info('Loaded Model-2:%s',model_2_path)

xgb_model_1 = joblib.load(model_1_path)
xgb_model_2 = joblib.load(model_2_path)

logger.info('Load Configs - Loaded success!')

##################################
#        Fetch Data       #
#################################
spark_session = SparkSession.builder.enableHiveSupport().appName("M_Model").config("spark.driver.memory","10g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)

outbound_features = hc.sql('''
    SELECT 
        mobile, c_age, c_city, c_sex,
        d_fir_dealfail_d, d_last_dealfail_d, m_oppr_fail_count as d_deal_fail_times,
        m_last_lead_first_source as first_resource_name, m_last_lead_second_source as second_resource_name, 
        d_last_leads_time, m_lead_source_count as count_of_resource, d_leads_count,
        d_card_ttl, d_leads_dtbt_count, d_fir_card_time as d_first_card_time, m_cust_max_level as lowest_cust_level,
        d_visit_ttl, m_last_visit_time as d_last_visit_time,
        d_trail_attend_ttl, d_activity_ttl, m_failed_called as outbound_vol,
        m_focus_series_count as d_leads_car_model_count
    FROM 
        marketing_modeling.app_big_wide_info 
    WHERE 
        mobile REGEXP "^[1][3-9][0-9]{9}$"
        AND d_deal_flag = 0
        AND d_last_dealfail_d >= date_add(to_date(from_unixtime(UNIX_TIMESTAMP("%s",'yyyyMMdd'))), -180)
        AND pt = "%s"
'''%(pt_date,pt_date)).toPandas()

# Merge data
outbound_features['c_city_level'] = outbound_features.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['city_level']
outbound_features['c_province'] = outbound_features.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['province']

info = outbound_features[['mobile','d_last_dealfail_d','c_province','c_city','c_sex','c_age','second_resource_name']]

logger.info('Fetch Data - Loaded success!')

                
##################################
#     Feature Engineering    #
#################################
# deal with datetime features
datetime_col = ['d_last_leads_time','d_first_card_time','d_fir_dealfail_d','d_last_dealfail_d','d_last_visit_time']
for col in datetime_col:
    outbound_features[col] = pd.to_datetime(outbound_features[col])         
outbound_features['d_fircard_dealf_diff'] = (outbound_features['d_last_dealfail_d']-outbound_features['d_first_card_time']).apply(lambda x: x.days)
outbound_features['d_fircard_firdealf_diff'] = (outbound_features['d_fir_dealfail_d']-outbound_features['d_first_card_time']).apply(lambda x: x.days)
outbound_features['d_dealf_lastvisit_diff'] = (outbound_features['d_last_dealfail_d']-outbound_features['d_last_visit_time']).apply(lambda x: x.days)
outbound_features['fircard_ob_diff'] = (outbound_date-outbound_features['d_first_card_time']).apply(lambda x: x.days)
outbound_features['lastfollow_ob_diff'] = (outbound_date-outbound_features['d_last_leads_time']).apply(lambda x: x.days)
outbound_features['fail_ob_diff'] = (outbound_date-outbound_features['d_last_dealfail_d']).apply(lambda x: x.days)
logger.info('Feature Engineering - Datediff features processing success!')

# deal with categorical features
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

## demographical features 
outbound_features['c_age'] = outbound_features['c_age'].apply(string_to_list_1)
outbound_features['c_city'] = outbound_features['c_city'].apply(string_to_list_1)
outbound_features['c_age'] = outbound_features['c_age'].map({u'20岁以下':0,u'21-25岁':1,u'26-30岁':2,u'31-35岁':3,u'36-40岁':4,u'41-45岁':5,u'46-50岁':6,u'51-55岁':7,u'56-60岁':8,u'60岁以上':9})
outbound_features['c_city_level'] = outbound_features['c_city_level'].map({u'一线城市':0,u'新一线城市':1,u'二线城市':2,u'三线城市':3,u'四线城市':4,u'五线城市':5})
## leads source features
outbound_features['first_resource_name'] = outbound_features['first_resource_name'].map({u'SIS接口分配':1,u'厂方导入':2,u'厂方的网销其他平台':3,u'广告投放':4,u'第一方触点':5,u'经销商网销主动开拓':6,u'网站平台接口':7})
outbound_features = outbound_features.join(pd.get_dummies(outbound_features['first_resource_name'],prefix="R1",prefix_sep="_"),on = None)
outbound_features['second_resource_name'] = outbound_features['second_resource_name'].map({u"汽车之家":1,u"易车网":2,u"懂车帝":3,u"null":5}).fillna(4)
outbound_features = outbound_features.join(pd.get_dummies(outbound_features['second_resource_name'],prefix="R2",prefix_sep="_"))
## customer graded level
outbound_features['lowest_cust_level'] = [np.nan if i is None else int(i[-1]) for i in outbound_features.lowest_cust_level]
## hostorical outbound performance
outbound_features["call_month"] = outbound_date.month
outbound_features["outbound_vol"] = outbound_features["outbound_vol"].map({0:1,1:0})
#E number of null features
outbound_features['null_vol'] = outbound_features[['c_city_level','c_sex','c_age','d_visit_ttl'
                                                   ,'d_trail_attend_ttl','d_activity_ttl']].isna().sum(axis=1)
logger.info('Feature Engineering - Categorical features processing success!')

##################################
#        Model Running     #
#################################
# deal with N/A
outbound_features= outbound_features.set_index('mobile')
cols_use = pd.read_csv(config['cols_used']).cols_used
missing_cols = set(cols_use) - set(outbound_features.columns)
for c in missing_cols:
    outbound_features[c] = -1
logger.info('Model Running - missing cols are:{}'.format(missing_cols))

# make prediction
outbound_features_slc = outbound_features[outbound_features['fail_ob_diff']<=180]
X = outbound_features_slc[cols_use]
X = X.fillna(-1)
def score(df):
    pred = zip(df['pred1'],df['pred2'])
    df['pred'] = [min(i) if max(i)<=0.75 else max(i) if min(i)>=0.85 else 0.1*i[0]+0.9*i[1] for i in pred]
    return df['pred']
X['pred1'] = list(pd.DataFrame(xgb_model_1.predict_proba(X[cols_use]))[1])
X['pred2'] = list(pd.DataFrame(xgb_model_2.predict_proba(X[cols_use]))[1])
X['pred_score'] = score(X)
logger.info('Model Running - Prediction success!')

result = pd.DataFrame(X.pred_score)
result = result.reset_index()
result['result_date'] = pd.datetime.now().strftime('%Y%m%d')
result_slc = result.sort_values(by = 'pred_score',ascending=False).reset_index(drop=True)
result_slc['score_rank'] = result_slc.index + 1
logger.info('Model Running - lowest pred score with duplicates:{}'.format(result_slc['pred_score'].min()))

# map features
result_slc_info = result_slc[['mobile','pred_score','score_rank']].merge(info, on = 'mobile', how='left')
result_slc_info = result_slc_info.replace(np.nan, " ")

print(result_slc_info.head())