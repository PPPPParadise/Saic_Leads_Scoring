# coding=UTF-8<code>

"""
Engineering of our datasets: 
* 
Created on Fri Jun 22 2020
@author: Xiaofeng XU
"""

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from datetime import datetime
import yaml
import logging

###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Data_Engineering_Others")
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

##################################################
# Feature Engineering of Non-Autohome Customers #
################################################
class DataEngineeringOthers:
    def __init__(self, data):

        self.data = data
   
    def de_others(self):
        
        # load configs
        with open('config.yaml') as config_file:
            config = yaml.full_load(config_file)
               
        remained_col = config['model_others_col']['remain_col']
        boundary_file_path = config['model_others_feature_engineering']['model_others_bin_boundary']
        datetime_list = config['model_others_feature_engineering']['datetime_list']
        binary_features = config['model_others_feature_engineering']['binary_features']
        city_lift = config['model_others_feature_engineering']['city_liftmatrix_result']
               
               
        # load data
        self.data = self.data[[i for i in list(pd.read_csv(remained_col)['col_used']) if i in self.data.columns]]
        logger.warning('Features %s were not imported!', \
                       str([i for i in list(pd.read_csv(remained_col)['col_used']) if i not in self.data.columns]))
              
        # deal with datetime features
        for col in datetime_list:
            self.data[col] = [1 if i.isoweekday() >= 6 else 2 if isinstance(i.isoweekday(), int) == True \
                                         else np.nan for i in pd.to_datetime(self.data[col])]
        
        # convert string to int by replacing
        str_feature_dic = {'d_cust_type':{60261001:1, 60261002:2}, 
                     'c_sex':{'性别为男性':1, '性别为女性':2},
                     'c_age':{'20岁以下':1, '21-25岁':3, '26-30岁':3, '31-35岁':3, '36-40岁':3, 
                           '41-45岁':3, '46-50岁':2, '51-55岁':2, '56-60岁':2, '60岁以上':4},
                     'c_province':{'贵州省':1, '云南省':1, '四川省':1, '重庆市':1, '福建省':2, '海南省':2, 
                              '广西壮族自治区':2, '安徽省':2, '江西省':2, '广东省':2, '上海市':2, 
                              '西藏自治区':2, '湖北省':2, '江苏省': 2},
                     'c_city_level':{'一线城市':3, '新一线城市':2, '二线城市':3, '三线城市':3, '四线城市':1, '五线城市':1},
                     'd_is_deposit_order':{0:2}}               
        transfer_list = ['d_cust_type', 'c_sex', 'c_age', 'c_province', 'c_city_level', 'd_is_deposit_order']
        for col in transfer_list:
            try:
                self.data[col] = self.data[col].replace(str_feature_dic[col])
            except:
                logger.warning('Feature %s is all None', col)                

        # deal with province
        self.data['c_province'] = [0 if i is np.nan else 3 if isinstance(i, int) == False else i for i in self.data['c_province']]

        # deal with city
        self.data['c_city'] = self.data[['c_city']].merge(pd.read_csv(city_lift), left_on = 'c_city', right_on = 'city', how = 'left')['lift_type']
        
        # binary column
        binary_features = [i.encode('utf-8') for i in binary_features]
        binary_features = [i for i in binary_features if i in self.data.columns]
        self.data[binary_features] = self.data[binary_features].where(self.data[binary_features].isnull(),1).fillna(0).astype(int)
        
        # bin numerical column
        with open(boundary_file_path) as dict_file:
            boundary_dict = yaml.full_load(dict_file)
        boundary_col = [i for i in boundary_dict.keys() if i in self.data.columns]
        for col in boundary_col:
            self.data[col] = pd.cut(self.data[col],boundary_dict[col])       
        
        logger.info('Non-Autohome feature engineering end!')

        return self.data