# coding=UTF-8<code>

"""
Engineering of our datasets: 
* Keeping only the actions with cover rate over 10% given the cover rate result
* Transforming the continuous variables into categorical variables given the lift matrix
Created on Fri Jun 19 2020
@author: Ruoyu ZHU
"""

import numpy as np
import pandas as pd
import yaml
import logging

###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Data_Engineering_Auto")
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
# Feature Engineering of Autohome Customers #
############################################
class DataEngineering:
    def __init__(self, wide_info_auto):

        self.wide_info_auto = wide_info_auto
   
    def de_auto(self):
        
        # load configs
        with open('config.yaml') as config_file:
            config = yaml.full_load(config_file)

        datetime_list = config['model_auto_feature_engineering']['model_auto_datetime_list']
        datetime_list_bi = config['model_auto_feature_engineering']['model_auto_datetime_list_bi']
        datetime_dic_lst = config['model_auto_feature_engineering']['model_auto_datetime_dic_lst']
        datetime_list_multi = config['model_auto_feature_engineering']['model_auto_datetime_list_multi']
        province_dic_list = config['model_auto_feature_engineering']['model_auto_province_dic_list']
        bins_for_multi = config['model_auto_feature_engineering']['model_auto_bins_for_multi']
        cols_to_multi = config['model_auto_feature_engineering']['model_auto_cols_to_multi']
        cols_to_bi = config['model_auto_feature_engineering']['model_auto_cols_to_bi']

            
        def datatime_to_weekday_and_month(data,col,col_wk_lst,col_mon_lst):
            """
            Transfer datetime columns in list into month and weekday classes or binary classes given:
            date(dataframe): input dataframe
            col(string): list of column name of the dataframe containing the actions
            col_wk_lst(string): output of weekday columns list
            col_mon_lst(string): output of month columns list
            """
            if col in data.columns:
                try:
                    data[col] = pd.to_datetime(data[col],format = '%Y-%m-%d %H:%M:%S')
                    col_name_wk = col + '_weekday'
                    col_wk_lst.append(col_name_wk)
                    data[col_name_wk] = [i.isoweekday() if i is not None else np.na for i in data[col]]
                    col_name_mon = col + '_month'
                    data[col_name_mon] = data[col].dt.month
                    col_mon_lst.append(col_name_mon)
                except:
                    logger.warning('The column %s was failed to be transferred!',col)
            return data, col_wk_lst, col_mon_lst

        def cols_to_multi_class(df,col_lst,bins_lst,binary = False):
            """
            Transfer columns in list into multiple classes or binary classes given:
            df(dataframe): input dataframe
            col_lst(string): list of column name of the dataframe containing the actions
            bins_lst(string/None): list of bins used to cut each column
            binary(bool): bool calue indicating whether to transfer into binary classes or multiple classes
           """
            if binary == False:
                for i in range(len(col_lst)):
                    col = col_lst[i]
                    if col in df.columns:
                        bins = bins_lst[i]
                        df[col] = pd.cut(df[col],bins = bins)
            else:
                for col in col_lst:
                    if col in df.columns:
                        df[col] = df[col].where(df[col].isnull(),1).fillna(0).astype(int)
            return df

        
        def cols_to_multi_value(df,col_lst,dic_lst):
            for i in range(len(col_lst)):
                dic_for_col = {}
                col = col_lst[i]
                if col in df.columns:
                    dic = dic_lst[i]
                    for k in range(len(dic)):
                        for j in dic[k]:
                            dic_for_col[j]=str(dic[k])
                    df[col] = df[col].apply(lambda x: dic_for_col[x] if x >= 0 else np.nan)
            return df
        
        
        #transform datetime columns 
        col_wk_lst = []
        col_mon_lst = []
        for col in datetime_list:
            self.wide_info_auto,col_wk_lst,col_mon_lst = datatime_to_weekday_and_month(self.wide_info_auto,col,col_wk_lst,col_mon_lst)
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto, datetime_list_bi, datetime_list_bi, binary = True)   
        self.wide_info_auto = cols_to_multi_value(self.wide_info_auto, datetime_list_multi, datetime_dic_lst)
        logger.info('Auto datetime transformation end!')
        
        #transform province column
        province_dic_list = {k.encode('utf-8'): v for k, v in province_dic_list.items()}
        self.wide_info_auto['c_province'] = self.wide_info_auto['c_province'].\
        apply(lambda x: province_dic_list[x] if isinstance(x, str) else np.nan)
        logger.info ('Auto province transformation end!')        
            
        #transform age column
        age_dic = {}
        age_dic['20岁以下'] = pd.Interval(left=0, right=20)
        age_dic['21-25岁'] = pd.Interval(left=20, right=25)
        age_dic['26-30岁'] = pd.Interval(left=25, right=30)
        age_dic['31-35岁'] = pd.Interval(left=30, right=35)
        age_dic['36-40岁'] = pd.Interval(left=35, right=60)
        age_dic['41-45岁'] = pd.Interval(left=35, right=60)
        age_dic['46-50岁'] = pd.Interval(left=35, right=60)
        age_dic['51-55岁'] = pd.Interval(left=35, right=60)
        age_dic['56-60岁'] = pd.Interval(left=35, right=60)
        age_dic['60岁以上'] = pd.Interval(left=60, right=100)

        self.wide_info_auto['c_age'] = self.wide_info_auto['c_age'].apply(lambda x: age_dic[x] if isinstance(x, str) else np.nan)
        logger.info ('Auto age transformation end!')

        #transform other multi-class columns
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto, cols_to_multi,bins_for_multi, binary = False)
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto, cols_to_bi, None, binary = True)
        logger.info ('Auto multiclass transformation end!')
        
        logger.info('Autohome feature engineering end!')
            
        return self.wide_info_auto