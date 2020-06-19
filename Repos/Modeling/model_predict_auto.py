#!/usr/bin/env python
# coding: utf-8

"""
Predicting purchasing probability of dataset: 
* Spliting dateset into dependent variables and independent variables
* Droping columns with high missing rate
* Onehot-encoding multi-class variables
* Imputing missing values with constant
* Oversampling minority data
* Training classification model
* Evaluating the result

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
from sklearn.utils import resample
from imblearn.over_sampling import SMOTE, ADASYN
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import *
from sklearn import tree
from sklearn.externals import joblib

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.insert(1, '/path/to/application/app/folder')


##############################################
#          Autohome Model           #
############################################
class DataPredicting():
    def __init__(self,data):
        self.data = data
        
    def feature_engineering(self):

        # create dummy columns
        dummy_col = ['h_level', 'h_level_cat', 'h_volume', 'c_sex', 'c_city_level', 'h_produce_way', 'c_age', 'd_fir_sec_leads_diff',
        'd_avg_leads_date', 'd_avg_visit_date', 'd_avg_fircard_firvisit_diff',  'd_fircard_firvisit_diff',
         'd_last_dfail_dealf_diff', 'd_dealf_lastvisit_diff', 'd_firlead_dealf_diff', 'd_lastlead_dealf_diff', 
        'd_dealf_firvisit_diff', 'd_lasttrail_dealf_diff', 'd_leads_dtbt_coincide',
        'd_leads_dtbt_ppt', 'd_deal_fail_ppt', 'h_focusing_avg_diff', 'h_focusing_max_diff', 'd_leads_dtbt_count', 'd_leads_dtbt_level_1', 
        'd_leads_count', 'd_trail_book_tll', 'h_model_nums', 'd_visit_ttl', 'd_activity_ttl', 'h_ttl_inquiry_time','d_card_ttl',                       'd_fir_leads_time_month','d_fir_card_time_month','d_fir_visit_time_month','d_fir_trail_month','d_last_reservation_time_month',
          'd_fir_dealfail_d_month','d_last_dealfail_d_month','d_fir_visit_time_weekday','d_last_dealfail_d_weekday',
          'd_followup_d7','d_followup_d15','d_followup_d30','d_followup_d60', 'd_followup_d90']
        
        self.X = self.data
        self.X = pd.get_dummies(data = self.X, columns = [i for i in self.X.columns if i in dummy_col])        
        
        # drop columns with highly imbalanced distribution
        pop_low_list = [
            "d_cust_type",
            "d_visit_dtbt_count",
            "d_is_deposit_order",
            "c_last_activity_time",
            "h_loan",
            "h_compete_car_num_d30",
            "h_compete_car_num_d60",
            "h_func_prefer3",
            "h_func_prefer8",
            "h_car_type_prefer4",
            "h_config_prefer1",
            "c_vertical_media",
            "c_leads_source_nums",
            "d_fir_dealfail_d_weekday",
            "d_fir_sec_leads_diff_(0.0, 858.0]",
            "d_leads_count_(10.0, 20.0]",
            "d_leads_count_(100.0, 821.0]",
            "d_leads_count_(20.0, 50.0]",
            "d_leads_count_(3.0, 4.0]",
            "d_leads_count_(4.0, 5.0]",
            "d_leads_count_(5.0, 6.0]",
            "d_leads_count_(50.0, 100.0]",
            "d_leads_count_(6.0, 7.0]",
            "d_leads_count_(7.0, 8.0]",
            "d_leads_count_(8.0, 9.0]",
            "d_leads_count_(9.0, 10.0]",
            "d_leads_dtbt_count_(10.0, 20.0]",
            "d_leads_dtbt_count_(20.0, 296.0]",
            "d_leads_dtbt_count_(3.0, 4.0]",
            "d_leads_dtbt_count_(4.0, 5.0]",
            "d_leads_dtbt_count_(5.0, 6.0]",
            "d_leads_dtbt_count_(6.0, 7.0]",
            "d_leads_dtbt_count_(7.0, 8.0]",
            "d_leads_dtbt_count_(8.0, 9.0]",
            "d_leads_dtbt_count_(9.0, 10.0]",
            "d_leads_dtbt_coincide_(0.0, 0.01]",
            "d_leads_dtbt_coincide_(0.01, 0.5]",
            "d_leads_dtbt_coincide_(0.5, 0.8]",
            "d_leads_dtbt_coincide_(0.8, 0.9]",
            "d_leads_dtbt_level_1_(-1e-06, 0.0]",
            "d_leads_dtbt_level_1_(10.0, 20.0]",
            "d_leads_dtbt_level_1_(20.0, 291.0]",
            "d_leads_dtbt_level_1_(3.0, 4.0]",
            "d_leads_dtbt_level_1_(4.0, 5.0]",
            "d_leads_dtbt_level_1_(5.0, 6.0]",
            "d_leads_dtbt_level_1_(6.0, 7.0]",
            "d_leads_dtbt_level_1_(7.0, 8.0]",
            "d_leads_dtbt_level_1_(8.0, 9.0]",
            "d_leads_dtbt_level_1_(9.0, 10.0]",
            "d_card_ttl_(10.0, 285.0]",
            "d_card_ttl_(3.0, 4.0]",
            "d_card_ttl_(4.0, 5.0]",
            "d_card_ttl_(5.0, 6.0]",
            "d_card_ttl_(6.0, 7.0]",
            "d_card_ttl_(7.0, 8.0]",
            "d_card_ttl_(8.0, 9.0]",
            "d_card_ttl_(9.0, 10.0]",
            "d_visit_ttl_(0.0, 1.0]",
            "d_visit_ttl_(1.0, 169.0]",
            "d_avg_visit_date_(-1e-06, 0.0]",
            "d_avg_visit_date_(0.0, 412.5]",
            "d_trail_book_tll_(-1e-06, 1.0]",
            "d_trail_book_tll_(1.0, 10.0]",
            "d_trail_book_tll_(10.0, 205.0]",
            "d_avg_fircard_firvisit_diff_(-1e-06, 0.0]",
            "d_avg_fircard_firvisit_diff_(-579.0, -1e-06]",
            "d_avg_fircard_firvisit_diff_(0.0, 855.0]",
            "d_activity_ttl_(0.0, 1.0]",
            "d_activity_ttl_(1.0, 106.0]",
            "d_followup_d7_(10.0, 15.0]",
            "d_followup_d7_(15.0, 20.0]",
            "d_followup_d7_(2.0, 3.0]",
            "d_followup_d7_(20.0, 209.0]",
            "d_followup_d7_(3.0, 4.0]",
            "d_followup_d7_(4.0, 5.0]",
            "d_followup_d7_(5.0, 6.0]",
            "d_followup_d7_(6.0, 7.0]",
            "d_followup_d7_(7.0, 8.0]",
            "d_followup_d7_(8.0, 9.0]",
            "d_followup_d7_(9.0, 10.0]",
            "d_followup_d15_(11.0, 15.0]",
            "d_followup_d15_(15.0, 20.0]",
            "d_followup_d15_(20.0, 348.0]",
            "d_followup_d15_(5.0, 7.0]",
            "d_followup_d15_(7.0, 9.0]",
            "d_followup_d15_(9.0, 11.0]",
            "d_followup_d30_(-1e-06, 1.0]",
            "d_followup_d30_(10.0, 15.0]",
            "d_followup_d30_(15.0, 20.0]",
            "d_followup_d30_(20.0, 30.0]",
            "d_followup_d30_(30.0, 50.0]",
            "d_followup_d30_(50.0, 954.0]",
            "d_followup_d60_(-1e-06, 1.0]",
            "d_followup_d60_(20.0, 30.0]",
            "d_followup_d60_(30.0, 40.0]",
            "d_followup_d60_(40.0, 50.0]",
            "d_followup_d60_(50.0, 1104.0]",
            "d_followup_d90_(-1e-06, 1.0]",
            "d_followup_d90_(20.0, 30.0]",
            "d_followup_d90_(30.0, 40.0]",
            "d_followup_d90_(40.0, 50.0]",
            "d_followup_d90_(50.0, 60.0]",
            "d_followup_d90_(60.0, 1104.0]",
            "d_firlead_dealf_diff_(-1e-06, 0.0]",
            "d_firlead_dealf_diff_(0.0, 1.0]",
            "d_firlead_dealf_diff_(1.0, 2.0]",
            "d_firlead_dealf_diff_(2.0, 3.0]",
            "d_firlead_dealf_diff_(3.0, 4.0]",
            "d_firlead_dealf_diff_(4.0, 5.0]",
            "d_firlead_dealf_diff_(5.0, 6.0]",
            "d_firlead_dealf_diff_(6.0, 7.0]",
            "d_firlead_dealf_diff_(7.0, 8.0]",
            "d_firlead_dealf_diff_(8.0, 9.0]",
            "d_firlead_dealf_diff_(9.0, 10.0]",
            "d_lastlead_dealf_diff_(-1e-06, 0.0]",
            "d_lastlead_dealf_diff_(0.0, 1.0]",
            "d_lastlead_dealf_diff_(1.0, 2.0]",
            "d_lastlead_dealf_diff_(2.0, 3.0]",
            "d_lastlead_dealf_diff_(3.0, 4.0]",
            "d_lastlead_dealf_diff_(4.0, 5.0]",
            "d_lastlead_dealf_diff_(5.0, 6.0]",
            "d_lastlead_dealf_diff_(6.0, 7.0]",
            "d_lastlead_dealf_diff_(7.0, 8.0]",
            "d_lastlead_dealf_diff_(8.0, 9.0]",
            "d_lastlead_dealf_diff_(9.0, 10.0]",
            "d_last_dfail_dealf_diff_(-1e-06, 0.0]",
            "d_last_dfail_dealf_diff_(-847.0, -1e-06]",
            "d_lasttrail_dealf_diff_(-1e-06, 0.0]",
            "d_lasttrail_dealf_diff_(0.0, 1.0]",
            "d_lasttrail_dealf_diff_(1.0, 2.0]",
            "d_lasttrail_dealf_diff_(10.0, 30.0]",
            "d_lasttrail_dealf_diff_(2.0, 3.0]",
            "d_lasttrail_dealf_diff_(3.0, 4.0]",
            "d_lasttrail_dealf_diff_(30.0, 650.0]",
            "d_lasttrail_dealf_diff_(4.0, 6.0]",
            "d_lasttrail_dealf_diff_(6.0, 7.0]",
            "d_lasttrail_dealf_diff_(7.0, 8.0]",
            "d_lasttrail_dealf_diff_(8.0, 9.0]",
            "d_lasttrail_dealf_diff_(9.0, 10.0]",
            "d_leads_dtbt_ppt_(0.0, 0.01]",
            "d_leads_dtbt_ppt_(0.01, 0.1]",
            "d_leads_dtbt_ppt_(0.1, 0.2]",
            "d_leads_dtbt_ppt_(0.2, 0.3]",
            "d_leads_dtbt_ppt_(0.3, 0.5]",
            "d_leads_dtbt_ppt_(0.5, 0.7]",
            "d_leads_dtbt_ppt_(0.7, 1.0]",
            "d_leads_dtbt_ppt_(1.0, 9.0]",
            "d_leads_dtbt_ppt_(9.0, 18.0]",
            "d_fircard_firvisit_diff_(-1e-06, 0.0]",
            "d_fircard_firvisit_diff_(0.0, 863.0]",
            "h_model_nums_(0.0, 1.0]",
            "h_model_nums_(6.0, 22.0]",
            "h_focusing_avg_diff_(-100.0, -90.0]",
            "h_focusing_avg_diff_(-104.0, -100.000001]",
            "h_focusing_avg_diff_(-1e-06, 60.0]",
            "h_focusing_avg_diff_(-90.0, -1e-06]",
            "h_focusing_avg_diff_(60.0, 80.0]",
            "h_focusing_max_diff_(-100.0, -90.0]",
            "h_focusing_max_diff_(-1e-06, 20.0]",
            "h_focusing_max_diff_(-90.0, -1e-06]",
            "h_focusing_max_diff_(20.0, 40.0]",
            "h_volume_1.0L及以下",
            "h_volume_2.1-2.5L",
            "h_volume_2.6-3.0L",
            "h_volume_3.1-4.0L",
            "h_volume_4.0L以上",
            "h_ttl_inquiry_time_(-1e-06, 0.0]",
            "h_ttl_inquiry_time_(1.0, 2.0]",
            "h_ttl_inquiry_time_(2.0, 3.0]",
            "h_ttl_inquiry_time_(3.0, 4.0]",
            "h_ttl_inquiry_time_(4.0, 5.0]",
            "h_ttl_inquiry_time_(5.0, 78.0]",
            "h_produce_way_进口",
            "h_level_cat_MPV",
            "h_level_cat_微卡",
            "h_level_cat_微面",
            "h_level_cat_皮卡",
            "h_level_cat_跑车",
            "h_level_cat_轻客",
            "c_age_(0, 20]",
            "c_age_(20, 25]",
            "c_age_(25, 30]",
            "c_age_(30, 35]",
            "c_age_(35, 60]",
            "c_age_(60, 100]",
            "c_sex_性别为女性",
            "c_city_level_一线城市",
            "c_city_level_五线城市",
            "d_fir_leads_time_month_[11]",
            "d_fir_leads_time_month_[2]",
            "d_fir_leads_time_month_[4, 12]",
            "d_fir_card_time_month_[11]",
            "d_fir_card_time_month_[2]",
            "d_fir_card_time_month_[4, 12]",
            "d_fir_visit_time_weekday_[1, 2, 3, 4, 5]",
            "d_fir_visit_time_weekday_[6]",
            "d_fir_visit_time_weekday_[7]",
            "d_fir_visit_time_month_[1, 4]",
            "d_fir_visit_time_month_[2, 3, 6, 9, 10, 11, 12]",
            "d_fir_visit_time_month_[5, 7, 8]",
            "d_fir_trail_month_[10]",
            "d_fir_trail_month_[12]",
            "d_fir_trail_month_[1]",
            "d_fir_trail_month_[2, 3, 6]",
            "d_fir_trail_month_[4, 5, 7]",
            "d_fir_trail_month_[8]",
            "d_fir_trail_month_[9, 11]",
            "d_last_reservation_time_month_[1, 4, 5, 6, 10, 11, 12]",
            "d_last_reservation_time_month_[2, 3, 7, 8, 9]",
            "d_last_dealfail_d_weekday_[6]",
            "d_last_dealfail_d_weekday_[7]"
        ]
        self.X = self.X.drop([i for i in self.X.columns if i in pop_low_list], axis = 1)
        
        training_columns = [
            "d_avg_visit_dtbt_count",
            "d_leads_car_model_count",
            "d_leads_car_model_type",
            "h_have_car",
            "h_compete_car_num_d90",
            "h_func_prefer1",
            "h_func_prefer5",
            "h_func_prefer6",
            "h_car_type_prefer1",
            "h_car_type_prefer3",
            "h_config_prefer2",
            "h_config_prefer3",
            "h_config_prefer6",
            "h_config_prefer7",
            "h_config_prefer9",
            "h_config_prefer11",
            "h_config_prefer13",
            "h_config_prefer14",
            "h_config_prefer16",
            "h_config_prefer18",
            "c_province",
            "c_MGHS",
            "c_MGZS",
            "c_MG6",
            "d_fir_trail_weekday",
            "d_last_reservation_time_weekday",
            "d_fir_sec_leads_diff_(-1e-06, 0.0]",
            "d_leads_count_(-1e-06, 0.0]",
            "d_leads_count_(0.0, 1.0]",
            "d_leads_count_(1.0, 2.0]",
            "d_leads_count_(2.0, 3.0]",
            "d_avg_leads_date_(-1e-06, 0.0]",
            "d_avg_leads_date_(0.0, 429.0]",
            "d_leads_dtbt_count_(-1e-06, 0.0]",
            "d_leads_dtbt_count_(0.0, 1.0]",
            "d_leads_dtbt_count_(1.0, 2.0]",
            "d_leads_dtbt_count_(2.0, 3.0]",
            "d_leads_dtbt_count_(20.0, 350.0]",
            "d_leads_dtbt_coincide_(-1e-06, 0.0]",
            "d_leads_dtbt_coincide_(0.9, 1.0]",
            "d_leads_dtbt_level_1_(0.0, 1.0]",
            "d_leads_dtbt_level_1_(1.0, 2.0]",
            "d_leads_dtbt_level_1_(2.0, 3.0]",
            "d_leads_dtbt_level_1_(20.0, 341.0]",
            "d_card_ttl_(-1e-06, 0.0]",
            "d_card_ttl_(0.0, 1.0]",
            "d_card_ttl_(1.0, 2.0]",
            "d_card_ttl_(2.0, 3.0]",
            "d_card_ttl_(10.0, 334.0]",
            "d_visit_ttl_(-1e-06, 0.0]",
            "d_trail_book_tll_(10.0, 209.0]",
            "d_avg_fircard_firvisit_diff_(-663.0, -1e-06]",
            "d_activity_ttl_(-1e-06, 0.0]",
            "d_activity_ttl_(1.0, 157.0]",
            "d_followup_d7_(-1e-06, 1.0]",
            "d_followup_d7_(1.0, 2.0]",
            "d_followup_d15_(-1e-06, 1.0]",
            "d_followup_d15_(1.0, 3.0]",
            "d_followup_d15_(3.0, 5.0]",
            "d_followup_d30_(1.0, 5.0]",
            "d_followup_d30_(5.0, 10.0]",
            "d_followup_d60_(1.0, 10.0]",
            "d_followup_d60_(10.0, 20.0]",
            "d_followup_d90_(1.0, 10.0]",
            "d_followup_d90_(10.0, 20.0]",
            "d_firlead_dealf_diff_(10.0, 30.0]",
            "d_firlead_dealf_diff_(30.0, 876.0]",
            "d_lastlead_dealf_diff_(10.0, 30.0]",
            "d_lastlead_dealf_diff_(30.0, 867.0]",
            "d_last_dfail_dealf_diff_(0.0, 842.0]",
            "d_leads_dtbt_ppt_(-1e-06, 0.0]",
            "d_leads_dtbt_ppt_(0.0, 0.1]",
            "h_model_nums_(-1e-06, 0.0]",
            "h_model_nums_(1.0, 6.0]",
            "h_focusing_avg_diff_(-100.000001, -100.0]",
            "h_focusing_avg_diff_(80.0, 100.0]",
            "h_focusing_avg_diff_(100.0, 104.0]",
            "h_focusing_max_diff_(-100.000001, -100.0]",
            "h_focusing_max_diff_(40.0, 100.0]",
            "h_volume_1.1-1.6L",
            "h_volume_1.7-2.0L",
            "h_ttl_inquiry_time_(0.0, 1.0]",
            "h_ttl_inquiry_time_(5.0, 83.0]",
            "h_produce_way_合资",
            "h_produce_way_国产",
            "h_level_cat_SUV",
            "h_level_cat_轿车",
            "c_sex_性别为男性",
            "c_city_level_三线城市",
            "c_city_level_二线城市",
            "c_city_level_四线城市",
            "c_city_level_新一线城市",
            "d_fir_leads_time_month_[1, 3, 6, 7, 8]",
            "d_fir_leads_time_month_[5, 9, 10]",
            "d_fir_card_time_month_[1, 3, 6, 7, 8]",
            "d_fir_card_time_month_[5, 9, 10]",
            "d_fir_dealfail_d_month_[1, 5, 6, 9, 10, 11, 12]",
            "d_fir_dealfail_d_month_[2, 3]",
            "d_fir_dealfail_d_month_[4, 7, 8]",
            "d_last_dealfail_d_weekday_[1, 2, 3, 4, 5]",
            "d_last_dealfail_d_month_[1, 2, 4, 10, 11]",
            "d_last_dealfail_d_month_[3, 6, 7, 8, 9]",
            "d_last_dealfail_d_month_[5, 12]"
        ]
        def add_missing_dummy_columns(training_columns,X):

            missing_cols = set( training_columns ) - set( X.columns )
            for c in missing_cols:
                X[c] = 0
            return X

        def fix_columns(training_columns,X):  

            X = add_missing_dummy_columns( training_columns,X)

            # make sure we have all the columns we need
            print( 'Add missing cols:',set( training_columns ) - set( X.columns ) == set())

            extra_cols = set( X.columns ) - set( training_columns )

            if extra_cols:
                print ("extra columns:", extra_cols)

            return X

        self.X = fix_columns(training_columns,self.X)
    
        
        # Impute missing value with 0.5
        self.X = self.X.fillna(0.5)
        self.mobiles = self.X.index
        print('Preprocessing end!')
            
    def predicting(self, pca_filepath, model_filepath, result_path):
        self.pca = joblib.load(pca_filepath)
        self.rf_best_model = joblib.load(model_filepath)
        
        self.X = self.pca.fit_transform(self.X)
        self.Y = self.rf_best_model.predict_proba(self.X)
        result = pd.DataFrame([i[1] for i in self.Y],index = self.mobiles, columns = ['pred_score'])
        result['result_date'] = pd.datetime.now()
        print ('Predicted end!')
        
        return result
       
        
                