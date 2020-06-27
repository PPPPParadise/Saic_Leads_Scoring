#!/usr/bin/env python
# coding: utf-8

"""
Predicting purchasing probability of dataset

Created on Fri Jun 24 2020
@author: Xiaofeng XU
"""

import os
import pandas as pd
import numpy as np
import datetime
import yaml
import logging

import sklearn
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import *
from sklearn import tree
from sklearn.externals import joblib

import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')


###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Model_Predict_Others")
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


##############################################
#          Non-Autohome Model        #
############################################
class DataPredictingOthers():
    def __init__(self, data):
        self.data = data
        
    def feature_engineering(self):
        
        # load configs
        with open('config.yaml') as config_file:
            config = yaml.full_load(config_file)
        
        undum_col = config['model_others_col']['undum_col']
        selected_col = config['model_others_col']['selected_col']
        
        undum_col = [i.encode('utf-8') for i in undum_col]       
        
        # create dummy columns        
        self.X = self.data
        self.X = pd.get_dummies(data = self.X, columns = [i for i in self.X.columns if i not in undum_col])
        
        # keep columns that we want to keep
        selected_col = list(pd.read_csv(selected_col, delimiter = '\t')['col_used'])
        
        null_col = [i for i in selected_col if i not in self.X.columns]
        
        if len(null_col) == len(list(self.X.columns)):
            logger.warning('All input columns of model others are missing, stop predicting!')
            os.exit()
        else:       
            for col in null_col:
                self.X[col] = 0
            self.X = self.X[[i for i in self.X.columns if i in selected_col]]
            self.mobiles = self.X.index
            logger.info('Non-Autohome features preprocessed end!')

            
    def predicting(self, pca_filepath, model_filepath):
        self.pca = joblib.load(pca_filepath)
        self.rf_best_model = joblib.load(model_filepath)
        
        self.X = self.pca.fit_transform(self.X)
        self.Y = self.rf_best_model.predict_proba(self.X)
        result = pd.DataFrame([i[1] for i in self.Y], index = self.mobiles, columns = ['pred_score'])
        result = result.reset_index()
        result['result_date'] = pd.datetime.now()
        result['pt'] = datetime.date.today().strftime("%Y%m%d")
        logger.info('Non-Autohome predicted end!')
        
        return result