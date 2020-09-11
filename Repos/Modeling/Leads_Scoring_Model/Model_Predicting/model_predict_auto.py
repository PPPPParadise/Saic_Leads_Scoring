#!/usr/bin/env python
# coding: utf-8

"""
Predicting purchasing probability of dataset

Created on Fri Jun 19 2020
@author: Ruoyu ZHU
"""

import os
import pandas as pd
import numpy as np

import sklearn
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score, cross_validate, cross_val_predict, KFold, LeaveOneOut
from sklearn.utils.validation import column_or_1d
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import *
from sklearn import tree
from sklearn.externals import joblib
import datetime
import yaml
import logging

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Model_Predict_Auto")
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
#          Autohome Model           #
############################################
class DataPredicting():
    def __init__(self,data):
        self.X = data
        
    def feature_engineering(self):

        with open('config.yaml') as config_file:
            config = yaml.full_load(config_file)
        
        dummy_col = config['model_auto_col']['model_auto_dummy_col']
        training_cols_path = config['model_auto_col']['model_auto_training_cols']

        # create dummy columns
        self.X = pd.get_dummies(data = self.X, columns = [i for i in self.X.columns if i in dummy_col])        
        
        with open(training_cols_path) as trancol_file:
            training_cols = list(pd.read_csv(trancol_file)['training_columns'])
        
        def fix_columns(training_cols, X):  
            # make sure we have all the columns we need
            def add_missing_dummy_columns(training_cols, X):
                missing_cols = set(training_cols) - set(X.columns)
                if missing_cols:
                    logger.warning("There are missing columns to add: %s", str(list(missing_cols)))
                for c in missing_cols:
                    X[c] = 0.5
                return X, missing_cols            
            
            X, missing_cols = add_missing_dummy_columns(training_cols, X)
            X = X[training_cols]

            extra_cols = set(X.columns) - set(training_cols) 
            if extra_cols:
                logger.warning("There are extra columns: %s", str(list(extra_cols)))            
            return X, missing_cols
        
        self.X, missing_cols = fix_columns(training_cols, self.X)
        
        # check if all the columns are None or missing
        if len(list(missing_cols)) == len(list(self.X.columns)):
            logger.critical('All input columns of model others are missing!')
            sys.exit(1)
        elif list(self.X.T.isna().all(axis = 1).unique()) == [True]:
            logger.critical('All input columns are None!')
            sys.exit(1)
        else:
            pass
                 
        # Impute missing value with 0.5
        self.X = self.X.fillna(0.5)
        self.mobiles = self.X.index
        logger.info('Autohome features preprocessed end!')
        
        
    def predicting(self, pca_filepath, model_filepath):
        self.pca = joblib.load(pca_filepath)
        self.rf_best_model = joblib.load(model_filepath)
        self.X = self.pca.fit_transform(self.X)
        self.Y = self.rf_best_model.predict_proba(self.X)
        result = pd.DataFrame([i[1] for i in self.Y], index = self.mobiles, columns = ['pred_score'])
        result = result.reset_index()
        result['result_date'] = pd.datetime.now()
        logger.info('Autohome predicted end!')
        return result
