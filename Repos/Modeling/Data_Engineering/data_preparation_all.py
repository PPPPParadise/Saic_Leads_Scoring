#coding=utf-8

"""
Fetch data from big_wide_info table and carry out preprocessing: 
* Process semi-features of autohome features
* Process semi-features of CDP features
* Merge different data sources to generate a final feature table

Created on Fri Jun 24 2020
@author: Xiaofeng XU / Ruoyu ZHU
"""

import pandas as pd
import numpy as np
import yaml
import logging

import sys
stdi,stdo,stde = sys.stdin,sys.stdout,sys.stderr
reload(sys)
sys.stdin,sys.stdout,sys.stderr = stdi,stdo,stde
sys.setdefaultencoding('utf-8')

###########################################
#           Set up Logs          #
##########################################

logger = logging.getLogger("Data_Preparation_All")
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


################################################
# Semi-feature Procrssing about Autohome Data #
##############################################

class AutohomePreparation:
    
    def __init__(self, auto):

        self.auto = auto
        
    def auto_processed(self):
        
        def string_to_list(x, split_type):
            '''
            Convert string-type list to a list give:
            x(string): input string-type list    
            '''
            try:
                return [int(i) for i in x[1:-1].split(split_type)]
            except:
                return np.nan             

            
        def create_dummy_columns(df, col, lst):
            def get_dummy(i, x):
                try:
                    if i in x:
                        return 1
                    elif i not in x:
                        return 0
                except:
                    return np.nan                        
            name = col + str(1)
            for i in lst:
                name = col + str(i)
                df[name] = df[col].apply(lambda x:get_dummy(i, x))
            return df

        
        def get_budget_cat(x, item_location, split_left, split_right):           
            '''
            Split budget and then club them into five groups given:
            x (string): input budget
            item_location (int): location after split
            split_left (int): started position of slicing
            split_right (int): ended position of slicing
       
            '''
            def budget_cat(n):
                try:
                    if n > 25:
                        return '25w+'
                    elif n < 10:
                        return '10w以下'
                    elif n < 15:
                        return '10w-15w'
                    elif n >= 20:
                        return '20w-25w'
                    elif n >= 15 and n < 20:
                        return '15w-20w'
                except:
                    return np.nan                       
            
            try:
                return budget_cat(float(x.split('-')[item_location][split_left:split_right]))
            except:
                return np.nan
        
        
        def get_level_cat_all(x):
            '''
            Return the car level of a car given:
            x(string): input name of the car            
            '''
            def get_level_cat():
                #Get the car level of a car
                level_dic = {}
                level_matches = ['轿车','SUV','MPV','跑车','微面','微卡','轻客','皮卡']
                jiaoche=[u'紧凑型车',u'小型车',u'中型车',u'中大型车',u'大型车',u'微型车']
                SUV = [u'紧凑型SUV',u'小型SUV',u'中型SUV',u'中大型SUV',u'大型SUV']
                mpv = [u'MPV']
                paoche = [u'跑车']
                weimian = [u'微面']
                weika = [u'微卡']
                qingke = [u'轻客']
                pika = [u'低端皮卡',u'高端皮卡']
                all_list = [jiaoche, SUV, mpv, paoche, weimian, weika, qingke, pika]
                t = 0
                for lst in all_list:
                    for i in lst:
                        level_dic[i.encode('utf8')] = level_matches[t]
                    t += 1 
                return level_dic 
            
            item_df = get_level_cat()
            try:
                return get_level_cat()[x]
            except:
                return np.nan               
           
        # func_preference
        col = ['h_func_prefer', 'h_car_type_prefer', 'h_config_prefer']
        for i in col:
            self.auto[i] = self.auto[i].apply(lambda x:string_to_list(x,','))
        lst_param = [10, 7, 19]
        for i in col:
            self.auto = create_dummy_columns(self.auto, i, list(range(1, lst_param[col.index(i)]))) 
        
        # budget
        self.auto['h_budget_min_cat'] = self.auto['h_budget'].apply(lambda x:get_budget_cat(x, 0, 0, -4))
        self.auto['h_budget_max_cat'] = self.auto['h_budget'].apply(lambda x:get_budget_cat(x, 1, 1, -3))

        # car_level_preference
        level_values = self.auto['h_level'].unique()
        self.auto['h_level_cat'] = self.auto['h_level'].apply(lambda x: get_level_cat_all(x))
        self.auto = self.auto.drop(['h_func_prefer', 'h_car_type_prefer', 'h_config_prefer', 'h_budget', 'h_level'], axis = 1)
        
        logger.info('Autohome processing end!')        
        
        return self.auto
    

################################################
#   Semi-feature Procrssing about CDP Data   #
##############################################

class CDPPreparation:
    
    def __init__(self, cdp):

        self.cdp = cdp
 
    def cdp_processed(self):
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
               

        def string_to_list_2(x):
            '''
            Convert string-type list to a list give:
            x(string): input string-type list
            '''
            try:
                return x.split('|')
            except:
                return np.nan             

        def create_dummy_columns_2(df,col,lst):
            '''
            Create dummy columns and list of created columns given:
            df(dataframe): input dataframe
            col(string): col that needed to be tranferred
            lst(list)：list that contains index of dhummy columns
            '''
            def get_dummy(i, x):
                try:
                    if i in x:
                        return 1
                    elif i not in x:
                        return 0
                except:
                    return np.nan

            col_names = []
            for i in lst:
                name = col + "_" + str(i)
                col_names.append(name)
                df[name] = df[col].apply(lambda x: get_dummy(i,x))
            return df, col_names
        
        
        # load configs
        with open('config.yaml') as config_file:
            config = yaml.full_load(config_file)
            
        city_level = config['model_others_feature_engineering']['city_level']
        vertical_media = config['semi_feature_col']['vertical_media']
        offcial_online = config['semi_feature_col']['offcial_online']
        models = config['semi_feature_col']['models']
        cdp_columns = config['semi_feature_col']['cdp_columns']        
        
        # city and age        
        ct_lv = pd.read_csv(city_level, sep = '\t')
        self.cdp['c_city'] = self.cdp['c_city'].apply(string_to_list_1)
        ct_lv['city_name'] = ct_lv['city_name'].astype(str)
        self.cdp['c_city'] = self.cdp['c_city'].astype(str)
        self.cdp['c_city_level'] = self.cdp.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['city_level']
        self.cdp['c_province'] = self.cdp.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['province']
        self.cdp['c_age'] = self.cdp['c_age'].apply(string_to_list_1)
        self.cdp['c_sex'] = self.cdp['c_sex'].apply(string_to_list_1)
        
        # car_vel
        models = [i.encode('utf-8') for i in models]
        self.cdp['c_lead_model'] = self.cdp['c_lead_model'].apply(lambda x:string_to_list_2(x))
        self.cdp,lead_cols = create_dummy_columns_2(self.cdp, 'c_lead_model', models)
        self.cdp['c_trail_vel'] = self.cdp['c_trail_vel'].apply(lambda x:string_to_list_2(x))
        self.cdp, tral_cols = create_dummy_columns_2(self.cdp, 'c_trail_vel', models)
        
        for model in models:
            self.cdp['c_lead_model_' + model] = self.cdp['c_lead_model_' + model].fillna(0)
            self.cdp['c_trail_vel_' + model] = self.cdp['c_trail_vel_' + model].fillna(0.5)
            self.cdp[model + '_mid'] = self.cdp['c_lead_model_' + model]  + self.cdp['c_trail_vel_' + model]
            self.cdp['c_'+ model] = [0 if i == 0.5 else 1 if i == 1.5 else 2 if i == 2 else 3 for i in self.cdp[model + '_mid']]
        
        # last_reach_platform
        self.cdp['c_last_reach_platform'] = self.cdp['c_last_reach_platform'].apply(lambda x:string_to_list_2(x))
        col_list_plt = self.cdp['c_last_reach_platform'][(self.cdp['c_last_reach_platform'].isnull() == False) & \
                                         (self.cdp['c_last_reach_platform'] != None) & \
                                         (self.cdp['c_last_reach_platform'] != '')].to_list()
        self.cdp = create_dummy_columns_2(self.cdp, 'c_last_reach_platform', list(set([i for k in col_list_plt for i in k])))[0]
        
        # lead_source
        self.cdp['c_lead_sources'] = self.cdp['c_lead_sources'].apply(lambda x:string_to_list_2(x))

        col_list_ls = self.cdp['c_lead_sources'][(self.cdp['c_lead_sources'].isnull() == False) &\
                                    (self.cdp['c_lead_sources'] != None) &\
                                    (self.cdp['c_lead_sources'] != '')].to_list()
        self.cdp, source_cols = create_dummy_columns_2(self.cdp, 'c_lead_sources', list(set([i for k in col_list_ls for i in k])))
        
        # Vertical_media
        vertical_media = [i.encode('utf-8') for i in vertical_media]
        vertical_media = [i for i in vertical_media if i in self.cdp.columns]
        offcial_online = [i.encode('utf-8') for i in offcial_online]
        offcial_online = [i for i in offcial_online if i in self.cdp.columns]
        self.cdp['c_vertical_media'] = self.cdp[vertical_media].sum(axis = 1) if len(vertical_media) != 0 else 0
        self.cdp['c_offcial_online'] = self.cdp[offcial_online].sum(axis = 1) if len(offcial_online) != 0 else 0
        self.cdp.loc[self.cdp[self.cdp['c_lead_sources'].isnull() == True].index, 'vertical_media'] = np.nan
        self.cdp.loc[self.cdp[self.cdp['c_lead_sources'].isnull() == True].index, 'offcial_online'] = np.nan
        
        # lead_source_volumn
        self.cdp['c_leads_source_nums'] = self.cdp[source_cols].sum(axis =1)
        
        # exclude useless columns
        cdp_columns = [i.encode('utf-8') for i in cdp_columns]
        self.cdp = self.cdp[[i for i in cdp_columns if i in self.cdp.columns]]
        
        logger.info('CDP processed end!')
        
        return self.cdp
