# coding=UTF-8<code>
### 1 :
"""Engineering of our datasets: 
* Keeping only the actions with cover rate over 10% given the cover rate result
* Transforming the continuous variables into categorical variables given the lift matrix


"""

### 2 :
import numpy as np
import pandas as pd

### 3 :


class DataEngineering:
    def __init__(self,wide_path):
        """
        
        """
        with open(wide_path,'r') as file:
            self.wide_info_all = pd.read_csv(file, delimiter = ',', index_col = 0)
            file.close()
        self.wide_info_auto = self.wide_info_all[self.wide_info_all['c_lead_sources_汽车之家']==1]
        self.wide_info_auto['mobile']=self.wide_info_auto['mobile'].apply(lambda x:str(x))
        self.datetime_list = ['d_fir_leads_time', 'd_fir_card_time', 'd_fir_visit_time', 'd_fir_trail',
                       'd_last_reservation_time','d_fir_dealfail_d','d_last_dealfail_d', 'd_fir_activity_time']
        self.datetime_list_bi = ['d_fir_leads_time_weekday', 'd_fir_card_time_weekday', 'd_fir_trail_weekday', 
                         'd_last_reservation_time_weekday', 'd_fir_dealfail_d_weekday']
        self.datetime_list_multi = ['d_fir_visit_time_weekday', 'd_last_dealfail_d_weekday','d_fir_leads_time_month',
                             'd_fir_card_time_month','d_fir_visit_time_month', 'd_fir_trail_month',
                             'd_last_reservation_time_month', 'd_fir_dealfail_d_month', 'd_last_dealfail_d_month',
                             'd_fir_activity_time_month']
        self.cols_to_multi = ["d_fir_sec_leads_diff","d_avg_leads_date","d_avg_visit_date","d_avg_fircard_firvisit_diff",
                        "d_firleads_firvisit_diff","d_fircard_firvisit_diff","d_avg_firleads_firvisit_diff",
                        "d_last_dfail_dealf_diff","d_dealf_lastvisit_diff", "d_firlead_dealf_diff",
                        "d_lastlead_dealf_diff","d_dealf_firvisit_diff","d_lasttrail_dealf_diff",
                        "d_last_activity_dealf_diff","d_fir_activity_dealf_diff", "d_leads_dtbt_coincide",
                        "d_leads_dtbt_ppt","d_deal_fail_ppt", "h_focusing_avg_diff",
                        "h_focusing_max_diff", "d_leads_dtbt_count", "d_leads_dtbt_level_1",
                        "d_leads_count","d_trail_book_tll","h_model_nums",
                        "d_visit_ttl","d_followup_ttl","d_activity_ttl",
                        "h_max_inquiry_time","h_ttl_inquiry_time","d_card_ttl"]
        self.cols_to_bi = ["d_fir_leads_deal_diff_y","d_avg_visit_dtbt_count"]
    
        
    def get_auto_table(self):
        """
        
        """
        columns_all = self.wide_info_auto.columns
        self.autohome_feature_list_ad = [i for i in columns_all if i.split('_')[0]=='h']
        self.auto_miss = self.wide_info_auto[self.wide_info_auto[self.autohome_feature_list_ad].isnull().T.all()==True]
        self.wide_info_auto = self.wide_info_auto[self.wide_info_auto['mobile'].isin(self.auto_miss['mobile'].to_list())==False]
        #return self.wide_info_auto

    def drop_lowcov_features(self):
        """
          

        """
        col_drop = [
        "d_fir_sec_visit_diff",
        "d_trail_attend_ttl",
        "d_trail_attend_ppt",
        "d_dealf_succ_firvisit_diff",
        "d_dealf_succ_lastvisit_diff",
        "d_avg_fir_sec_visit_diff",
        "d_avg_firleads_firvisit_diff",
        "d_fir_activity_time",
        "d_fir_leads_deal_diff_y",
        "d_trail_count_d30",
        "d_trail_count_d90",
        "d_visit_count_d15",
        "d_visit_count_d30",
        "d_visit_count_d90",
        "d_activity_count_d15",
        "d_activity_count_d30",
        "d_activity_count_d60",
        "d_activity_count_d90",
        "d_firfollow_dealf_diff",
        "d_lastfollow_dealf_diff",
        "d_dealf_lastvisit_diff",
        "d_dealf_firvisit_diff",
        "d_firleads_firvisit_diff",
        "d_last_activity_dealf_diff",
        "d_fir_activity_dealf_diff",
        "d_fir_order_leads_diff",
        "d_fir_order_visit_diff",
        "d_fir_order_trail_diff",
        "c_last_sis_time",
        "c_register_time",
        "c_last_reach_platform_MG服务号",
        "c_last_reach_platform_MGAPP",
        "c_last_reach_platform_MG官网"]
        
        self.wide_info_auto = self.wide_info_auto.drop([i for i in self.wide_info_auto.columns if i in col_drop],axis = 1)
    
    def trans_datetime(self):
        """
        
        
        """
        #datetime to week and month
        self.col_wk_lst = []
        self.col_mon_lst = []
        
        def datatime_to_weekday_and_month(data, col,col_wk_lst,col_mon_lst):
            """
            
            """
            if col in data.columns:
                data[col] = pd.to_datetime(data[col],format = '%Y-%m-%d %H:%M:%S')
                col_name_wk = col + '_weekday'
                col_wk_lst.append(col_name_wk)
                data[col_name_wk] = [i.isoweekday() if i is not None else np.na for i in data[col]]
                col_name_mon = col + '_month'
                data[col_name_mon] = data[col].dt.month
                col_mon_lst.append(col_name_mon)
            return data,col_wk_lst,col_mon_lst

        for col in self.datetime_list:
            self.wide_info_auto,self.col_wk_lst,self.col_mon_lst = datatime_to_weekday_and_month(self.wide_info_auto,col,
                                                                     self.col_wk_lst,self.col_mon_lst)
        def get_binary_cat(df,col):
            """
            Create binary column with 1 indicting not null and 0 indicating null given:
            df(dataframe): input dataframe
            col: column used to transfer

            """
            index_list = df[df[col].isnull()==False].index
            df.loc[index_list,col] = 1
            df.loc[~df.index.isin(index_list),col] = 0
            return df[col]
        def multi_class(df,col,bins):
            '''
            Create multiple-class column by cutting column from bins given:
            df(dataframe): input dataframe
            col: column used to transfer
            bins: bins used to cut column
            '''
            table = df[(df[col].isnull()==False)]
            bins.append(table[col].max())
            bins.append(table[col].min())
            bins = list(set(bins))
            bins.sort()
            df[col]=pd.cut(table[col],bins = bins)
            return df[col]
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
                        df[col] = multi_class(df,col,bins)
            else:
                for col in col_lst:
                    if col in df.columns:
                        df[col] = get_binary_cat(df,col)

            return df
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto,self.datetime_list_bi,self.datetime_list_bi,binary = True)
        
 
                                                  
                                                  
        def cols_to_multi_value(df,col_lst,dic_lst):
            '''

            '''
            for i in range(len(col_lst)):
                #print i
                dic_for_col = {}
                col = col_lst[i]
                if col in df.columns:
                    dic = dic_lst[i]
                    for k in range(len(dic)):
                        for j in dic[k]:
                            dic_for_col[j]=str(dic[k])
                    df[col] = df[col].apply(lambda x: dic_for_col[x] if x>=0 else np.nan)
            return df
        self.dic_lst = [[[1,2,3,4,5],[6],[7]],
                [[1,2,3,4,5],[6],[7]], 
                [[2],[1,3,6,7,8],[5,9,10],[11],[4,12]],
                [[2],[1,3,6,7,8],[5,9,10],[11],[4,12]],
                [[5,7,8],[2,3,6,9,10,11,12],[1,4]],
                [[4,5,7],[2,3,6],[8],[9,11],[10],[12],[1]],
                [[2,3,7,8,9],[1,4,5,6,10,11,12]],
                [[2,3],[4,7,8],[1,5,6,9,10,11,12]],
                [[3,6,7,8,9],[1,2,4,10,11],[5,12]],
                [[4,5],[2,3,6,7,8],[1,9,10,11],[12]]]
        self.wide_info_auto = cols_to_multi_value(self.wide_info_auto,self.datetime_list_multi,self.dic_lst)
    
    def trans_province(self):
        """
        
        """
        col = 'c_province'

        dic_list = {
            "西藏自治区":1,"青海省":1,"上海市":1,"四川省":1,"贵州省":1,"安徽省":1,"河南省":1,"浙江省":1,"北京市":1,"广西壮族自治区":1,"山东省":1,
            "广东省":0, "江西省":0, "江苏省":0,"陕西省":0, "天津市":0,"云南省":0,"湖北省":0,"湖南省":0,"山西省":0,"福建省":0,"重庆市":0,
            "海南省":0,"内蒙古自治区":0,"河北省":0,"新疆维吾尔自治区":0,"吉林省":0,"宁夏回族自治区":0,"甘肃省":0, "辽宁省":0,"黑龙江省":0
            }

        self.wide_info_auto[col] = self.wide_info_auto[col].apply(lambda x:dic_list[x] if isinstance(x,str) else np.nan)
    
    
        
    def trans_age(self):
        """
        
        """
        def string_to_list_age(x):
            '''
            Ignore multiple ages
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

        col = 'c_age'
        self.wide_info_auto[col] = self.wide_info_auto[col].apply(string_to_list_age)

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

        self.wide_info_auto[col] = self.wide_info_auto[col].apply(lambda x: age_dic[x] if isinstance(x,str) else np.nan)
  
        
    def trans_multiclass(self):
        """
        
        """
        def get_binary_cat(df,col):
            """
            Create binary column with 1 indicting not null and 0 indicating null given:
            df(dataframe): input dataframe
            col: column used to transfer

            """
            index_list = df[df[col].isnull()==False].index
            df.loc[index_list,col] = 1
            df.loc[~df.index.isin(index_list),col] = 0
            return df[col]

        def multi_class(df,col,bins):
            """
            Create multiple-class column by cutting column from bins given:
            df(dataframe): input dataframe
            col: column used to transfer
            bins: bins used to cut column
            """
            table = df[(df[col].isnull()==False)]
            bins.append(table[col].max())
            bins.append(table[col].min())
            bins = list(set(bins))
            bins.sort()
            df[col]=pd.cut(table[col],bins = bins)
            return df[col]

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
                        df[col] = multi_class(df,col,bins)
            else:
                for col in col_lst:
                    if col in df.columns:
                        df[col] = get_binary_cat(df,col)

            return df
        
        bins_for_multi = [[-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0],    
                    [-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10,30],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10,30],
                    [-0.000001,0,2,3,10,30],
                    [-0.000001,0,1,2,3,4,6,7,8,9,10,30],
                    [-0.000001,0,1,2,3,4,6,8,9,10,30],
                    [-0.000001,0,1,2,3,4,6,8,9,10,30],
                    [-0.000001,0,0.5,0.8,0.9,1],
                    [-0.000001,0,0.1,0.2,0.3,0.5,0.7,1,9],
                    [-0.000001,0,0.9,1],
                    [-100.000001,-100,-90,-0.000001,60,80,100],
                    [-100.000001,-100,-90,-0.000001,20,40],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10,20],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10,20],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10,20,50,100],
                    [-0.000001,10],
                    [-0.000001,0,1,6],
                    [-0.000001,0,1],
                    [-0.000001,0,1],
                    [-0.000001,0,1],
                    [-0.000001,0],
                    [-0.000001,0,1,2,3,4,5],
                    [-0.000001,0,1,2,3,4,5,6,7,8,9,10]]
        
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto,self.cols_to_multi,bins_for_multi,binary = False)
        self.wide_info_auto = cols_to_multi_class(self.wide_info_auto,self.cols_to_bi,None,binary = True)
        
        
    def select_cols(self):
        """
        
        """
        
        select_lst = [
                "mobile",
                "d_fir_sec_leads_diff",
                "d_leads_count",
                "d_avg_leads_date",
                "d_leads_dtbt_count",
                "d_leads_dtbt_coincide",
                "d_leads_dtbt_level_1",
                "d_cust_type",
                "d_card_ttl",
                "d_visit_dtbt_count",
                "d_visit_ttl",
                "d_avg_visit_date",
                "d_avg_visit_dtbt_count",
                "d_trail_book_tll",
                "d_leads_car_model_count",
                "d_leads_car_model_type",
                "d_is_deposit_order",
                "d_deal_flag",
                "d_avg_fircard_firvisit_diff",
                "d_activity_ttl",
                "d_followup_d7",
                "d_followup_d15",
                "d_followup_d30",
                "d_followup_d60",
                "d_followup_d90",
                "d_firlead_dealf_diff",
                "d_lastlead_dealf_diff",
                "d_last_dfail_dealf_diff",
                "d_lasttrail_dealf_diff",
                "d_leads_dtbt_ppt",
                "d_fircard_firvisit_diff",
                "c_last_activity_time",
                "h_loan",
                "h_model_nums",
                "h_have_car",
                "h_compete_car_num_d30",
                "h_compete_car_num_d60",
                "h_compete_car_num_d90",
                "h_focusing_avg_diff",
                "h_focusing_max_diff",
                "h_volume",
                "h_ttl_inquiry_time",
                "h_produce_way",
                "h_func_prefer1",
                "h_func_prefer3",
                "h_func_prefer5",
                "h_func_prefer6",
                "h_func_prefer8",
                "h_car_type_prefer1",
                "h_car_type_prefer3",
                "h_car_type_prefer4",
                "h_config_prefer1",
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
                "h_level_cat",
                "c_age",
                "c_sex",
                "c_province",
                "c_city_level",
                "c_MGHS",
                "c_MGZS",
                "c_MG6",
                "c_vertical_media",
                "c_leads_source_nums",
                "d_fir_leads_time_month",
                "d_fir_card_time_month",
                "d_fir_visit_time_weekday",
                "d_fir_visit_time_month",
                "d_fir_trail_weekday",
                "d_fir_trail_month",
                "d_last_reservation_time_weekday",
                "d_last_reservation_time_month",
                "d_fir_dealfail_d_weekday",
                "d_fir_dealfail_d_month",
                "d_last_dealfail_d_weekday",
                "d_last_dealfail_d_month"]
        self.wide_info_auto = self.wide_info_auto[select_lst]
            
    def save_features(self,save_path):
        self.wide_info_auto.to_csv(save_path,index = False)
        

        
        
#DataEngineering = DataEngineering(wide_path = '/home/mam_jupyter/jupyter_dir/artefact/leads_scoring_model/Python/dev/feature_all.csv')    

#DataEngineering.get_auto_table()

#DataEngineering.drop_lowcov_features()

#DataEngineering.trans_datetime()

#DataEngineering.trans_province()

#DataEngineering.trans_age()

#DataEngineering.trans_multiclass()

#DataEngineering.select_cols()

#print ('Data engineering end')

#save_path = '/home/mam_jupyter/jupyter_dir/artefact/leads_scoring_model/Python/dev/feature_auto.csv'

#DataEngineering.save_features(save_path)

#print ('Data saving end')