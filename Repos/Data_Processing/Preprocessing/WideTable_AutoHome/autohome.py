# coding:utf-8

import json
import sys
import datetime
from pyspark import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType


# UDF
def get_config_prefer(arr):
    prefer = []
    for s in arr.encode('utf-8').split('、'):
        if '汽油' in s or '柴油' in s or '电动' in s or '油电混合' in s:
            prefer.append(s.decode('utf-8'))
    return '|'.join(prefer)


def get_car_usage(arr):
    usage = []
    arr = arr.encode('utf-8')
    for s in arr.split('、'):
        if s != '换车增购':
            usage.append(s.decode('utf-8'))
    return '|'.join(usage)


def is_change_car_and_buy(arr):
    if '换车增购' not in arr.encode('utf-8'):
        return 0
    else:
        return 1


def get_models(focus_car, inquiry_30, inquiry_60, inquiry_90):
    car = json.loads(focus_car)
    inq_30 = json.loads(inquiry_30)
    inq_60 = json.loads(inquiry_60)
    inq_90 = json.loads(inquiry_90)
    result = set()
    for item in car:
        for key in item.keys():
            result.add(key)
    for item in inq_30:
        result.add(item.get('name'))
    for item in inq_60:
        result.add(item.get('name'))
    for item in inq_90:
        result.add(item.get('name'))
    return '|'.join(result)


def get_models_size(focus_car, inquiry_30, inquiry_60, inquiry_90):
    car = json.loads(focus_car)
    inq_30 = json.loads(inquiry_30)
    inq_60 = json.loads(inquiry_60)
    inq_90 = json.loads(inquiry_90)
    result = set()
    for item in car:
        for key in item.keys():
            result.add(key)
    for item in inq_30:
        result.add(item.get('name'))
    for item in inq_60:
        result.add(item.get('name'))
    for item in inq_90:
        result.add(item.get('name'))
    return len(result)


def get_inquiry_size(inquiry):
    result = set()
    nums = json.loads(inquiry)
    for item in nums:
        result.add(item.get('name'))
    return len(result)


def get_proportion(focus_car, arg):
    cars = json.loads(focus_car)
    others = 0
    mg = 0
    if arg == 'avg':
        for car in cars:
            for name in car.keys():
                proportion = int(car.get(name))
                name = name.encode('utf-8')
                if 'MG' in name or 'mg' in name or '名爵' in name:
                    mg += proportion
                else:
                    others += proportion
    else:
        for car in cars:
            for name in car.keys():
                proportion = int(car.get(name))
                name = name.encode('utf-8')
                if 'MG' in name or 'mg' in name or '名爵' in name:
                    if proportion > mg:
                        mg = proportion
                else:
                    if proportion > others:
                        others = proportion
    return others - mg


def get_list(items, arg):
    item_list = items.encode('utf-8').split('、')
    result = []
    func = {
        '空间': 1, '外观': 2, '性价比': 3, '操控': 4, '动力': 5, '内饰': 6, '能耗': 7, '舒适性': 8, '油耗': 9
    }
    car_tp = {
        '工薪代步': 1, '公用商务': 2, '越野自驾': 3, '换车增购': 4, '家庭出游': 5, '豪华风范': 6
    }
    config = {
        '全景天窗': 1, '电动天窗': 2, '电动调节座椅': 3, '车身ESP': 4, '疝气大灯': 5, 'GPS导航': 6, '定速巡航': 7,
        '真皮座椅': 8, '倒车雷达': 9, '全自动空调': 10, '多功能方向盘': 11, 'LED大灯': 12, '倒车影像': 13, '无钥匙启动': 14,
        '座椅加热': 15, '日间行车灯': 16, '自动泊车': 17, '蓝牙/车载电话': 18
    }
    if arg == 'func':
        for item in item_list:
            it = func.get(item, 'nothing')
            if it != 'nothing':
                result.append(it)
    elif arg == 'conf':
        for item in item_list:
            it = config.get(item, 'nothing')
            if it != 'nothing':
                result.append(it)
    elif arg == 'car':
        for item in item_list:
            it = car_tp.get(item, 'nothing')
            if it != 'nothing':
                result.append(it)

    return ''.join(str(result))


def get_level_or_volume(config, arg):
    conf_list = config.encode('utf-8').split('、')
    if len(conf_list) > 2:
        if arg == 'level':
            return conf_list[0].decode('utf-8')
        elif arg == 'volume':
            if 'L' in conf_list[1]:
                return conf_list[1].decode('utf-8')
            else:
                return ''


def get_real_budget(budget):
    budget = float(budget.strip().split('-')[1].strip()[0:-1])
    if budget < 10:
        return '<10w'
    elif 10 <= budget < 15:
        return '10-15w'
    elif 15 <= budget < 20:
        return '15-20w'
    elif 20 < budget < 25:
        return '20-25w'
    else:
        return '>25w'


def get_inquiry_time(inq, arg):
    inq = json.loads(inq)
    res = 0
    if arg == 'ttl':
        for item in inq:
            res += int(item.get('num'))
    elif arg == 'max':
        for item in inq:
            if int(item.get('num')) > res:
                res = item.get('num')
    return res


def get_production(config):
    config = config.encode('utf-8')
    if '国产' in config:
        return '国产'.decode('utf-8')
    if '进口' in config:
        return '进口'.decode('utf-8')
    if '合资' in config:
        return '合资'.decode('utf-8')
    return ''


def get_config_size(config):
    config = config.encode('utf-8')
    return len(config.split('、'))


def running(pt):
    spark_session = SparkSession \
        .builder.enableHiveSupport() \
        .appName("Artefact-data-flow") \
        .config("spark.driver.memory", "10g") \
        .getOrCreate()

    hc = HiveContext(spark_session.sparkContext)
    hc.udf.register("get_config_prefer", get_config_prefer, StringType())
    hc.udf.register("get_car_usage", get_car_usage, StringType())
    hc.udf.register("get_models", get_models, StringType())
    hc.udf.register("get_models_size", get_models_size, IntegerType())
    hc.udf.register("get_inquiry_size", get_inquiry_size, IntegerType())
    hc.udf.register("get_proportion", get_proportion, IntegerType())
    hc.udf.register("get_list", get_list, StringType())
    hc.udf.register("get_level_or_volume", get_level_or_volume, StringType())
    hc.udf.register("is_change_car_and_buy", is_change_car_and_buy, IntegerType())
    hc.udf.register("get_real_budget", get_real_budget, StringType())
    hc.udf.register("get_inquiry_time", get_inquiry_time, IntegerType())
    hc.udf.register("get_production", get_production, StringType())
    hc.udf.register("get_config_size", get_config_size, IntegerType())

    # 获取所有汽车之家线索
    sql = """
        select 
            mobile as id,
            car_type as goal,
            loan,
            car_type as priority_car_type,
            configuration_preference,
            get_config_prefer(configuration_preference) as priority_config_prefer,
            get_car_usage(car_type) as priority_car_usage,
            functional_preference as client_focus,
            get_models(focus_car, inquiry_model_30, inquiry_model_60, inquiry_model_90) as models,
            get_models_size(focus_car, inquiry_model_30, inquiry_model_60, inquiry_model_90) as model_nums,
            have_car,
            compete,
            get_inquiry_size(inquiry_model_30) as compete_car_num_d30,
            get_inquiry_size(inquiry_model_60) as compete_car_num_d60,
            get_inquiry_size(inquiry_model_90) as compete_car_num_d90,
            get_proportion(focus_car,'avg') as focusing_avg_diff,
            get_proportion(focus_car,'max') as focusing_max_diff,
            get_list(functional_preference, 'func') as func_prefer,
            get_list(car_type, 'car') as car_type_prefer,
            get_list(configuration_preference, 'conf') as config_prefer,
            budget,
            displace,
            get_level_or_volume(configuration_preference, 'level') as level,
            get_level_or_volume(configuration_preference, 'volume') as volume,
            is_change_car_and_buy(configuration_preference) as priority_change_car_and_buy,
            get_inquiry_time(inquiry_model_90, 'ttl') as ttl_inquiry_time,
            get_inquiry_time(inquiry_model_90, 'max') as max_inquiry_time,
            get_production(configuration_preference) as produce_way,
            get_config_size(configuration_preference) as config_nums,
            leads_create_time
        from dtwarehouse.user_portrait
        """
#    sql = sql % (pt)
    data = hc.sql(sql)
    data.createOrReplaceTempView("aotohome_tmp")
    createsql = """
        create table marketing_modeling.tmp_autohome_behavior as
            select * from aotohome_tmp
            """
    dropsql = "drop table if exists marketing_modeling.tmp_autohome_behavior"
    hc.sql(dropsql)
    hc.sql(createsql)


if __name__ == "__main__":
    pt = ''
    """
	if len(sys.argv) == 2:
        pt=sys.argv[1]
    else:
        date_time = datetime.datetime.now() - datetime.timedelta(days=1)
        pt = date_time.strftime("%Y%m%d")
	"""
    running(pt)
