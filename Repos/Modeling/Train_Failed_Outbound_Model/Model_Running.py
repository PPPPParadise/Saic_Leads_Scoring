#!/usr/bin/env python
# coding: utf-8

'''
Training Pipeline of MG Failed Outbound Model
Created on Mon Oct 26 2020
@author: Xiaofeng XU
'''

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, cross_validate, cross_val_predict, KFold, LeaveOneOut
import xgboost as xgb
from sklearn.metrics import *
import joblib
import logging
import sys
import yaml

###########################################
#           Set up Logs          #
##########################################
logger = logging.getLogger("Model_Training")
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
xgb_parameter = config['xgb_parameters']
xgb_model_1_path = config['model_files']['model_path'] + pt_date +  '_' + config['model_files']['xgb_model_1'] # source stored model file
xgb_model_2_path = config['model_files']['model_path'] + pt_date +  '_' + config['model_files']['xgb_model_2'] # source stored model file
logger.info('Load Configs - Loaded success!')

##################################
#        Model Training       #
#################################
def split_data(df,brand,train_size,seed):
    df[cols_used] = df[cols_used].fillna(-1) # fill N/A
    if brand == True: # filter RW data
        train_x,test_x,train_y,test_y = train_test_split(df[df.brand=='RW'][cols_used],df[df.brand=='RW']['ob_result']
                                                         ,train_size = train_size,random_state = seed)
    else:
        train_x,test_x,train_y,test_y = train_test_split(df[cols_used],df['ob_result']
                                                         ,train_size = train_size,random_state = seed)        
    return train_x,test_x,train_y,test_y
    
def model_runing(train_x,train_y,model_parameters,model_save_path):
    # Model Runing
    xgb_model = xgb.XGBClassifier(**model_parameters)
    xgb_model.fit(train_x, train_y)
    joblib.dump(xgb_model,model_save_path)
    return xgb_model

def model_performance(test_x,test_y,thres,model_1,model_2):     
    test_y = pd.DataFrame(test_y)
    test_y['pred_1'] = [i[1] for i in model_1.predict_proba(test_x)]
    test_y['pred_2'] = [i[1] for i in model_2.predict_proba(test_x)]
    
    def score(df,thres):
        pred = zip(df['pred_1'],df['pred_2'])
        df['pred'] = [min(i) if max(i)<=0.75 else max(i) if min(i)>=0.85 else 0.1*i[0]+0.9*i[1] for i in pred]
        df['pred'] = [1 if i >= thres else 0 for i in df['pred']]
        return df    
    test_y = score(test_y,0.5)
    
    performance_matrix = pd.DataFrame([[accuracy_score(test_y.ob_result,test_y.pred),precision_score(test_y.ob_result,test_y.pred)
                                        ,recall_score(test_y.ob_result,test_y.pred),f1_score(test_y.ob_result,test_y.pred)]]
                                      ,columns = ['accuracy','precision','recall','f1'])
    return performance_matrix

# fetch data
outbound_features = pd.read_csv('Training_Data/outbound_features.csv')
logger.info('Load %s Data!', str(outbound_features.shape))
    
# run RW
train_rw_x,test_rw_x,train_rw_y,test_rw_y = split_data(outbound_features,True,0.8,123)
xgb_model_1 = model_runing(train_rw_x,train_rw_y,xgb_parameter,xgb_model_1_path)
logger.info('Model 1 Saved Successed')
    
# run all
train_x,test_x,train_y,test_y = split_data(outbound_features,False,0.8,123)
xgb_model_2 = model_runing(train_x,train_y,xgb_parameter,xgb_model_2_path)
logger.info('Model 2 Saved Successed')
    
# model performance
model_performance = model_performance(test_x,test_y,0.5,xgb_model_1,xgb_model_2)
model_performance['train_date'] = pt_date
previous_performance = pd.read_csv('Model_Performance.csv')
previous_performance.append(model_performance).to_csv('Model_Performance.csv')
logger.info('Model Performance Saved') 
