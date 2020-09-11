#!/usr/bin/env python
# coding: utf-8

'''
MG Failed Outbound Model
Created on Fri Aug 31 2020
@author: Xiaofeng XU
'''

import pandas as pd
import numpy as np
import joblib
from datetime import datetime
import logging
import sys
import paramiko
import csv
import yaml

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType

###########################################
#           Set up Logs          #
##########################################
logger = logging.getLogger("Outbound_Info_Generation")
logger.setLevel(logging.DEBUG) 

fh = logging.FileHandler("MG_fail_outbound_model.log")
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
outbound_date = sys.argv[2]
golden_dl = pd.read_excel(config['input_data']['golden_dealer_list'])

###########################################
#            Fetch Data          #
##########################################
spark_session = SparkSession.builder.enableHiveSupport().appName("M_Model").config("spark.driver.memory","10g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)

cust_dealer_info = hc.sql('select * from marketing_modeling.ob_cust_dealer_info where rn = 1').toPandas()
dealer_info = hc.sql('''
select 
    dlm_org_id,
    dealer_code,
    parent_dealer_code,
    parent_dealer_shortname,
    address,
    city_name,
    area
from dtwarehouse.ods_rdp_v_sales_region_dealer
where pt = "%s"
'''%(pt_date)).toPandas()
logger.info('Dealer info read success!')


cust_dealer_info['is_direct_follow'] = config['outbound_list_info']['is_direct_follow']
cust_dealer_info['oppor_brand'] = config['outbound_list_info']['oppor_brand']
cust_dealer_info['first_source'] = config['outbound_list_info']['first_source']
cust_dealer_info['second_source'] = config['outbound_list_info']['second_source']
cust_dealer_info['activity_name'] = config['outbound_list_info']['activity_name']
cust_dealer_info['scirpt_id'] = config['outbound_list_info']['scirpt_id']
cust_dealer_info['adress'] = config['outbound_list_info']['adress']
cust_dealer_info['oppor_purchase_time'] = config['outbound_list_info']['oppor_purchase_time']
cust_dealer_info['own_vehicle'] = config['outbound_list_info']['own_vehicle']
cust_dealer_info['existing_vehicles_age'] = config['outbound_list_info']['existing_vehicles_age']
cust_dealer_info['fail_reason'] = cust_dealer_info['fail_reason'].fillna("[]")
cust_dealer_info['fail_reason'] = cust_dealer_info['fail_reason'].apply(lambda x:x[1:-1])
cust_dealer_info['fail_reason'] = cust_dealer_info['fail_reason'].apply(lambda x:''.join(x))
cust_dealer_info['pt'] = outbound_date
cust_dealer_info = cust_dealer_info.rename(columns = {'dealer_code':'detailed_dealer_code'})
cust_dealer_info = cust_dealer_info.rename(columns ={'parent_dealer_shortname':'dealer_shortname','parent_dealer_code':'dealer_code','address':'dealer_address'})
logger.info('Info rename success!')
cust_dealer_info['cust_label'] = cust_dealer_info.T.apply(lambda x :'{"'+'age":"'+x.age+'","sex":"'+x.sex+'","province":"'+x.province+
                                             '","city":"'+x.city+
                                             '","adress":"'+x.adress+
                                             '","fail_reason":"'+x.fail_reason+
                                             '","oppor_purchase_time":"'+x.oppor_purchase_time+
                                             '","own_vehicle":"'+x.own_vehicle+
                                             '","existing vehicles_age":"'+x.existing_vehicles_age+
                                             '","dealer_shortname":"'+x.dealer_shortname+
                                             '","dealer_code":"'+x.dealer_code+
                                             '","dealer_address":"'+x.dealer_address+
                                             '"}')

logger.info('Info complete success!')

###### 更换经销商 ######
golden_dl_docdic = dict(zip(golden_dl[u'市'], golden_dl[u'代码']))
golden_dl_namedic = dict(zip(golden_dl[u'市'], golden_dl[u'经销商简称']))
golden_dl_adddic = dict(zip(dealer_info['dealer_code'],dealer_info['address']))

cust_dealer_info['city_name'] = cust_dealer_info['city_name'].replace(u'上海市市辖区',u'上海市')
cust_dealer_info['金经销商id']=cust_dealer_info['city_name'].apply(lambda x:golden_dl_docdic[x] if x in golden_dl_docdic.keys() else np.nan)
cust_dealer_info['金经销商name']=cust_dealer_info['city_name'].apply(lambda x:golden_dl_namedic[x] if x in golden_dl_namedic.keys() else np.nan)
cust_dealer_info['dealer_code'] = [i if i is not np.nan else j for i,j in zip(cust_dealer_info['金经销商id'],cust_dealer_info['dealer_code'])]
cust_dealer_info['dealer_shortname'] = [i if i is not np.nan else j for i,j in zip(cust_dealer_info['金经销商name'],cust_dealer_info['dealer_shortname'])]
cust_dealer_info['dealer_address']=cust_dealer_info['dealer_code'].apply(lambda x:golden_dl_adddic[x] if x in golden_dl_adddic.keys() else np.nan)
logger.info('Info change success!')

###### Save result ######
cust_dealer_info_out = cust_dealer_info[['name','mobile','is_direct_follow','dealer_shortname','detailed_dealer_code','dealer_address','oppor_brand','oppor_series','first_source','second_source','activity_name','pt','fail_leads_oppor_score','fail_time','fail_leads_source','scirpt_id','cust_label']]
cust_dealer_info_out.to_csv('Output_Data/'+pt_date+'_outbound'+'.csv',quoting = csv.QUOTE_ALL,index = False,encoding = 'utf-8-sig')
logger.info('Info save success!')
print(cust_dealer_info_out.head())

###### Sent file ######
file_sent = 'Output_Data/'+pt_date+'_outbound'+'.csv'
transport = paramiko.Transport(("10.129.19.51", 8821))
transport.connect(username="appuser", password="zaq12wsx@banmapassw0rd")
sftp = paramiko.SFTPClient.from_transport(transport)
sftp.put(file_sent,"/home/appuser/leads_import/1_leads_table/"+pt_date+'_fail_outbound'+'.csv')
logger.info('Info sent success!')
transport.close()