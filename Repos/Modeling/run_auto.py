#!/usr/bin/env python
# coding: utf-8

'''
MG Customer Leads Scroing Model Running Pipeline

Created on Fri Jun 19 2020
@author: Xiaofeng XU / Ruoyu ZHU
'''

import pandas as pd
import numpy as np

from data_preparation_all import AutohomePreparation, CDPPreparation
from data_engineering_auto import DataEngineering
from model_predict_auto import DataPredicting


##########################################
#           Set Path           #
##########################################

# set input and output data path
input_path = 'mm_big_wide_info.csv'
result_path = 'ls_result.csv'

# set model files path
pca_filepath = 'pca.pkl'
model_filepath = 'rf_model.pkl'


##########################################
#          Fetch Data           #
##########################################

# Read data
names = [
    'mobile',
    'd_fir_leads_time',
    'd_fir_sec_leads_diff',
    'd_leads_count',
    'd_avg_leads_date',
    'd_leads_dtbt_count',
    'd_leads_dtbt_coincide',
    'd_leads_dtbt_level_1',
    'd_leads_dtbt_level_2',
    'd_cust_type',
    'd_fir_card_time',
    'd_card_ttl',
    'd_fir_visit_time',
    'd_fir_sec_visit_diff',
    'd_visit_dtbt_count',
    'd_visit_ttl',
    'd_avg_visit_date',
    'd_avg_visit_dtbt_count',
    'd_followup_ttl',
    'd_fir_trail',
    'd_last_reservation_time',
    'd_trail_book_tll',
    'd_trail_attend_ttl',
    'd_trail_attend_ppt',
    'd_leads_car_model_count',
    'd_leads_car_model_type',
    'd_fir_dealfail_d',
    'd_last_dealfail_d',
    'd_is_deposit_order',
    'd_deal_flag',
    'd_dealf_succ_firvisit_diff',
    'd_dealf_succ_lastvisit_diff',
    'd_avg_fir_sec_visit_diff',
    'd_fircard_firvisit_dtbt_diff',
    'd_avg_fircard_firvisit_diff',
    'd_avg_firleads_firvisit_diff',
    'd_fir_activity_time',
    'd_activity_ttl',
    'd_has_deliver_history',
    'd_trail_count_d30',
    'd_trail_count_d90',
    'd_visit_count_d15',
    'd_visit_count_d30',
    'd_visit_count_d90',
    'd_followup_d7',
    'd_followup_d15',
    'd_followup_d30',
    'd_followup_d60',
    'd_followup_d90',
    'd_activity_count_d15',
    'd_activity_count_d30',
    'd_activity_count_d60',
    'd_activity_count_d90',
    'd_firlead_dealf_diff',
    'd_lastlead_dealf_diff',
    'd_last_dfail_dealf_diff',
    'd_firfollow_dealf_diff',
    'd_lastfollow_dealf_diff',
    'd_dealf_lastvisit_diff',
    'd_dealf_firvisit_diff',
    'd_lasttrail_dealf_diff',
    'd_firleads_firvisit_diff',
    'd_leads_dtbt_ppt',
    'd_fircard_firvisit_diff',
    'd_last_activity_dealf_diff',
    'd_fir_activity_dealf_diff',
    'd_fir_order_leads_diff',
    'd_fir_order_visit_diff',
    'd_fir_order_trail_diff',
    'c_last_activity_time',
    'c_last_sis_time',
    'c_register_time',
    'c_age',
    'c_sex',
    'c_city',
    'c_lead_model',
    'c_trail_vel',
    'c_last_trail_vel',
    'c_deliver_vel',
    'c_last_reach_platform',
    'c_lead_sources',
    'h_goal',
    'h_loan',
    'h_model_nums',
    'h_have_car',
    'h_compete',
    'h_compete_car_num_d30',
    'h_compete_car_num_d60',
    'h_compete_car_num_d90',
    'h_focusing_avg_diff',
    'h_focusing_max_diff',
    'h_displace',
    'h_volume',
    'h_ttl_inquiry_time',
    'h_max_inquiry_time',
    'h_produce_way',
    'h_config_nums',
    'h_func_prefer',
    'h_car_type_prefer',
    'h_config_prefer',
    'h_budget',
    'h_level'
]
features = pd.read_csv(input_path, delimiter = '\t', header = None, names = names)
features ['mobile'] = features ['mobile'].astype(str)
auto_col = ['h_func_prefer','h_car_type_prefer','h_config_prefer','h_budget','h_level']
auto = features[['mobile'] + auto_col] 
cdp_col = ['c_age','c_sex','c_city','c_lead_model','c_trail_vel','c_last_trail_vel','c_deliver_vel','c_last_reach_platform', 'c_lead_sources']
cdp = features[['mobile'] + cdp_col] 
print ('Read all features end!')


##########################################
#       Feature Engineering        #
##########################################

# Semi-feature processing
auto_precessed = AutohomePreparation(auto)
auto = auto_precessed.auto_processed()

cdp_precessed = CDPPreparation(cdp)
cdp = cdp_precessed.cdp_processed()

feature_merge = features.drop(auto_col + cdp_col, axis = 1)
feature_all = feature_merge.merge(auto, how = 'inner', on = 'mobile').merge(cdp, how = 'inner', on = 'mobile')
feature_others = feature_all[feature_all[list(feature_all.columns[[i[0:2] == 'h_' for i in feature_all.columns]])].isna().T.all() == True]
feature_auto = feature_all[~feature_all['mobile'].isin(feature_others['mobile'])]
feature_others = feature_others.set_index('mobile')
feature_auto = feature_auto.set_index('mobile')
print (feature_all.shape, feature_auto.shape, feature_others.shape)

# autohome feature engineering
auto_engineer = DataEngineering(feature_auto)    
auto_data = auto_engineer.de_auto()
print ('Autohome feature engineering end!')


##########################################
#          Model Running         #
##########################################

# Run model
DataPredicting = DataPredicting(auto_data)
DataPredicting.feature_engineering()

# Output and save
result = DataPredicting.predicting(pca_filepath, model_filepath, result_path)
result = hc.createDataFrame(result)
result.withColumn("pt",pd.datetime.now())
result.write.saveAsTable("marketing_modeling.mm_model_result", format = "Hive", mode = "append", partitionBy = ["pt"])
print ('Result saved!')
