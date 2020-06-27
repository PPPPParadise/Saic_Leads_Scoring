#!/usr/bin/env python
# coding: utf-8

'''
MG Customer Leads Scroing Model Running Pipeline

Created on Fri Jun 23 2020
@author: Xiaofeng XU / Ruoyu ZHU
'''

import pandas as pd
import numpy as np
import datetime
import yaml
import logging

from Data_Engineering.data_preparation_all import AutohomePreparation, CDPPreparation
from Data_Engineering.data_engineering_auto import DataEngineering
from Model_Predicting.model_predict_auto import DataPredicting
from Data_Engineering.data_engineering_others import DataEngineeringOthers
from Model_Predicting.model_predict_others import DataPredictingOthers

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType
spark_session = SparkSession.builder.enableHiveSupport().appName("test").config("spark.driver.memory","30g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Model_Execution")
logger.setLevel(logging.DEBUG) 

fh = logging.FileHandler("MG_leads_scoring_model.log")
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

# get input data path
input_path = config['input_data']['big_wide_table']
input_data_columns = config['input_data_columns']

# get model files path
a_pca_filepath = config['model_files']['auto_pca']
a_model_filepath = config['model_files']['auto_model']
o_pca_filepath = config['model_files']['others_pca']
o_model_filepath = config['model_files']['others_model']


##########################################
#          Fetch Data           #
##########################################

# Read data
names = list(pd.read_csv(input_data_columns)['col_used'])
features = pd.read_csv(input_path, delimiter = '\t', header = None, names = names)
features ['mobile'] = features ['mobile'].astype(str)
auto_col = ['h_func_prefer','h_car_type_prefer','h_config_prefer','h_budget','h_level']
auto = features[['mobile'] + auto_col] 
cdp_col = ['c_age','c_sex','c_city','c_lead_model','c_trail_vel','c_last_reach_platform', 'c_lead_sources']
cdp = features[['mobile'] + cdp_col] 
logger.info('Read all features end!')


##########################################
#       Feature Engineering        #
##########################################

# Semi-feature processing
auto_precessed = AutohomePreparation(auto)
auto = auto_precessed.auto_processed()

cdp_precessed = CDPPreparation(cdp)
cdp = cdp_precessed.cdp_processed()


# Split data into two parts: Autohome & Non-Autohome
feature_merge = features.drop(auto_col + cdp_col, axis = 1)
feature_all = feature_merge.merge(auto, how = 'inner', on = 'mobile').merge(cdp, how = 'inner', on = 'mobile')
feature_others = feature_all[feature_all[list(feature_all.columns[[i[0:2] == 'h_' for i in feature_all.columns]])].isna().T.all() == True]
feature_auto = feature_all[~feature_all['mobile'].isin(feature_others['mobile'])]
feature_others = feature_others.set_index('mobile')
feature_auto = feature_auto.set_index('mobile')

# autohome feature engineering
auto_engineer = DataEngineering(feature_auto)    
auto_data = auto_engineer.de_auto()

# non-autohome feature engineering
others_engineer = DataEngineeringOthers(feature_others)    
others_data = others_engineer.de_others()

##########################################
#          Model Running         #
##########################################

# Run model
DataPredicting = DataPredicting(auto_data)
DataPredicting.feature_engineering()

DataPredictingOthers = DataPredictingOthers(others_data)
DataPredictingOthers.feature_engineering()

# Output and save
auto_result = DataPredicting.predicting(a_pca_filepath, a_model_filepath)
others_result = DataPredictingOthers.predicting(o_pca_filepath, o_model_filepath)

try:
    result = pd.concat([auto_result, others_result])
    result = hc.createDataFrame(result)
    result.write.insertInto("marketing_modeling.app_model_result")
    logger.info('Result saved!')
except:
    logger.critical('Prediction failed, please check !!!!!')