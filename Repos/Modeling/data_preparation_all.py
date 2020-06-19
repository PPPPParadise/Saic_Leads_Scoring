#coding=utf-8

"""
Fetch data from big_wide_info table and carry out preprocessing: 
* Process semi-features of autohome features
* Process semi-features of CDP features
* Merge different data sources to generate a final feature table

Created on Fri Jun 19 2020
@author: Xiaofeng XU / Ruoyu ZHU
"""

import pandas as pd
import numpy as np

import sys
stdi,stdo,stde = sys.stdin,sys.stdout,sys.stderr
reload(sys)
sys.stdin,sys.stdout,sys.stderr = stdi,stdo,stde
sys.setdefaultencoding('utf-8')


################################################
# Semi-feature Procrssing about Autohome Data #
##############################################

class AutohomePreparation:
    
    def __init__(self, auto):

        self.auto = auto
        
    def auto_processed(self):
        
        def string_to_list(x):
            '''
            Convert string-type list to a list give:
            x(string): input string-type list    
            '''
            if isinstance(x,int) | isinstance(x,float):
                return np.nan
            if len(x) == 2:
                return 0
            elif len(x) > 2:
                return [int(i) for i in x[1:-1].split(',')]


        def get_dummy(i, x):
            '''
            Check if i is in x given:
            i(int): input element need to be checked existing status
            x(list): input list that might contains i
            '''
            if isinstance(x,int) | isinstance(x,float):
                return np.nan
            elif i in x:
                return 1
            elif i not in x:
                return 0
            else:
                print('error')


        def create_dummy_columns(df, col, lst):
            '''
            '''
            name = col + str(1)
            for i in lst:
                name = col + str(i)
                df[name] = df[col].apply(lambda x:get_dummy(i, x))
            return df

        
        def get_budget_min_cat(x):
            '''
            Group the lower bound of budget given:
            x(string): input string      
            '''
            if x is None:
                return np.nan
            if isinstance(x,int) | isinstance(x, float) :
                return np.nan
            elif len(x)<=1:
                return np.nan
            else:
                mmin = float(x.split('-')[0][:-4])
                if mmin > 25:
                    return '25w+'
                elif mmin < 10:
                    return '10w以下'
                elif mmin < 15:
                    return '10w-15w'
                elif mmin >= 20:
                    return '20w-25w'
                elif mmin >= 15 and mmin < 20:
                    return '15w-20w'
                else:
                    print('error')

                    
        def get_budget_max_cat(x):
            '''
            Group the upper bound of budget given:
            x(string): input string    
            '''
            if x is None:
                return np.nan
            if isinstance(x,int) | isinstance(x,float) :
                return np.nan
            elif len(x) <= 1:
                return np.nan
            else:
                mmax = float(x.split('-')[1][1:-3])
                if mmax > 25:
                    return '25w+'
                elif mmax < 10:
                    return '10w以下'
                elif mmax < 15:
                    return '10w-15w'
                elif mmax >= 20:
                    return '20w-25w'
                elif mmax >= 15 and mmax < 20:
                    return '15w-20w'
                else:
                    print('error')
                    
        def get_level_cat():
            '''
            Get the car level of a car
            '''
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


        def get_level_cat_all(x):
            '''
            Return the car level of a car given:
            x(string): input name of the car
            '''
            item_df = get_level_cat()
            if isinstance(x,int) | isinstance(x,float):
                return np.nan
            elif x in get_level_cat().keys():
                return get_level_cat()[x]
            else:
                return np.nan
           
        # func_preference
        col = ['h_func_prefer', 'h_car_type_prefer', 'h_config_prefer']
        for i in col:
            self.auto[i] = self.auto[i].apply(string_to_list)
        lst_param = [10, 7, 19]
        for i in col:
            self.auto = create_dummy_columns(self.auto, i, list(range(1, lst_param[col.index(i)]))) 
        
        # budget
        self.auto['h_budget_min_cat'] = self.auto['h_budget'].apply(lambda x:get_budget_min_cat(x))
        self.auto['h_budget_max_cat'] = self.auto['h_budget'].apply(lambda x:get_budget_max_cat(x))

        # car_level_preference
        level_values = self.auto['h_level'].unique()
        self.auto['h_level_cat'] = self.auto['h_level'].apply(lambda x: get_level_cat_all(x))
        self.auto = self.auto.drop(['h_func_prefer', 'h_car_type_prefer', 'h_config_prefer', 'h_budget', 'h_level'], axis = 1)
        
        print ('Autohome processing end!')        
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
            if isinstance(x,str):
                if '|' in x:
                    return np.nan
                elif len(x) == 0:
                    return np.nan
                else:
                    return x
            else:
                return np.nan

        def string_to_list_2(x):
            '''
            Convert string-type list to a list give:
            x(string): input string-type list
            '''
            if isinstance(x,str):
                if len(x) == 0:
                    return np.nan
                else:
                    return x.split('|')
            else:
                return np.nan

        def get_dummy(i, x):
            '''
            Check if i is in x given:
            i(int): input element need to be checked existing status
            x(list): input list that might contains i
            '''
            if isinstance(x,int) | isinstance(x,float):
                return np.nan
            elif i in x:
                return 1
            elif i not in x:
                return 0
            else:
                print('error') 

        def create_dummy_columns_2(df,col,lst):
            '''
            Create dummy columns and list of created columns given:
            df(dataframe): input dataframe
            col(string): col that needed to be tranferred
            lst(list)：list that contains index of dhummy columns
            '''
            col_names = []
            for i in lst:
                name = col + "_" + str(i)
                col_names.append(name)
                df[name]=df[col].apply(lambda x: get_dummy(i,x))
            return df, col_names
        
        # city and age        
        ct_lv = pd.read_csv('city_level.txt', sep = '\t')
        self.cdp['c_city'] = self.cdp['c_city'].apply(string_to_list_1)
        ct_lv['city_name'] = ct_lv['city_name'].astype(str)
        self.cdp['c_city'] = self.cdp['c_city'].astype(str)
        self.cdp['c_city_level'] = self.cdp.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['city_level']
        #self.cdp['c_province'] = self.cdp['c_province'].apply(string_to_list_1)
        self.cdp['c_province'] = self.cdp.merge(ct_lv, how = 'left', left_on = 'c_city', right_on = 'city_name')['province']
        self.cdp['c_age'] = self.cdp['c_age'].apply(string_to_list_1)
        self.cdp['c_sex'] = self.cdp['c_sex'].apply(string_to_list_1)
        
        # car_vel
        models = ["MGeHS","MGGT","MG锐腾","MGeMGHS","MGHS","MGeMG6","MGZS纯电动","MGZS","MG6"]
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

        col_list_ls = self.cdp['c_lead_sources'][(self.cdp['c_lead_sources'].isnull() == False) & (self.cdp['c_lead_sources'] != None) \
                                              &(self.cdp['c_lead_sources'] != '')].to_list()
        self.cdp, source_cols = create_dummy_columns_2(self.cdp, 'c_lead_sources', list(set([i for k in col_list_ls for i in k])))
        
        # Vertical_media
        vertical_media = ['c_lead_sources_懂车帝','c_lead_sources_汽车之家','c_lead_sources_途虎','c_lead_sources_易车',
                  'c_lead_sources_17汽车','c_lead_sources_太平洋','c_lead_sources_车享CRM（品牌馆，又叫电商）','c_lead_sources_爱卡']
        offcial_online = ['c_lead_sources_名爵APP','c_lead_sources_官网']
        self.cdp['c_vertical_media'] = self.cdp[vertical_media].sum(axis = 1)
        self.cdp['c_offcial_online'] = self.cdp[offcial_online].sum(axis = 1)
        self.cdp.loc[self.cdp[self.cdp['c_lead_sources'].isnull() == True].index,'vertical_media'] = np.nan
        self.cdp.loc[self.cdp[self.cdp['c_lead_sources'].isnull() == True].index,'offcial_online'] = np.nan
        
        # lead_source_volumn
        self.cdp['c_leads_source_nums'] = self.cdp[source_cols].sum(axis =1)
        
        self.cdp = self.cdp[["mobile","c_city","c_age","c_sex","c_province","c_city_level","c_MGeHS","c_MGGT","c_MG锐腾","c_MGeMGHS","c_MGHS",
                      "c_MGeMG6","c_MGZS纯电动","c_MGZS","c_MG6","c_last_reach_platform_MG服务号","c_last_reach_platform_MGAPP",
                      "c_last_reach_platform_MG官网","c_lead_sources_汽车之家","c_vertical_media","c_offcial_online","c_leads_source_nums"]]
        print ('CDP processed end!')
        
        return self.cdp

