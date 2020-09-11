# coding:utf-8
import pandas as pd
import numpy as np
import yaml
import os
#from pyhive import hive
import re
import jieba
from zhtools.langconv import *
import logging
from collections import OrderedDict

from pyspark import SparkContext
from pyspark.sql import SparkSession,HiveContext,Window
from pyspark.sql import functions as fn
from pyspark.sql.types import IntegerType, FloatType, DoubleType, ArrayType, StringType, DecimalType,MapType

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

###################################
#       Set up Logs        #
#################################
logger = logging.getLogger("Failed_Reasons_Extraction")
logger.setLevel(logging.DEBUG) 

fh = logging.FileHandler("Failed_Reasons_Extraction.log")
fh.setLevel(logging.INFO) 
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

###########################################
#       Configs and dictionary           #
##########################################
clean_word_dict = {
    u'无':u'没'
    ,u'已经':u'已'
    ,u'未考虑':u'不考虑'
    ,u'补考虑':'不考虑'
    ,u'占时':u'暂时'
    ,u'赞不':u'暂时不'
    ,u'占不考虑':u'暂时不考虑'
    ,'MAILE':u'已买'
    ,u'合姿':u'合资'
    ,u'家人':u'家里人'
    ,u'取消':u'没'
    ,u'没有':u'没'
    ,u'以购':u'已购'
    ,u'以购车':u'已购'
    ,u'以购买':u'已购'
    ,u'卖完':u'已买'
    ,u'迈':u'买'
    ,u'卖掉':u'买掉'
    ,u'说话':u'讲话'
    ,u'木有':u'没'
    ,u'已构':u'已购'
    ,u'已够':u'已购'
    ,u'以提':u'已提'
    ,u'以订':u'已订'
    ,u'须要':u'需要'
    ,u'不须':u'不需'
    ,u'世家':u'试驾'
    ,u'无钱':u'没钱'
    ,'CUO WU':u'错误'
    ,u'经历':u'经理'
    ,u'7坐':u'7座'
    ,u'手机号':u'号码'
    ,u'一买':u'已买'
    ,u'两箱':u'两厢'
    ,u'姿源':u'资源'
    ,u'以后':u'后'
    # car brand
    ,'MG':u'名爵'
    ,'I5':u'荣威'
    ,'BYD':u'比亚迪'
    ,'BMW':u'宝马'
    ,u'三凌':u'三菱'
    ,u'科迪亚克':'柯迪亚克'
    ,u'卡迪拉克':u'凯迪拉克'
    ,u'私欲':u'思域'
    ,u'领肯':u'领克'
    ,u'领客':u'领克'
    ,u'凌克':u'领克'
    ,'CRV':u'本田'
    ,u'长按':u'长安'
    ,u'马3':u'马自达'
    ,'A3':u'奥迪'
    ,'H6':u'哈弗'
    ,u'哈佛':u'哈弗'
    ,u'哈费':u'哈弗'
    ,u'大纵':u'大众'
    ,'XRV':u'本田'
    ,'RX':u'雷克萨斯'
    ,'WEY':u'长城'
    ,u'魏':u'长城'
    ,u'魏派':u'长城'
    ,'VV5':u'长城'
    ,'VV6':u'长城'
    ,'VV7':u'长城'
    ,'NESSAN':u'尼桑'
    ,u'雪弗莱':u'雪佛兰'
    ,u'雪弗兰':u'雪佛兰'
    ,u'欧兰德':u'欧蓝德'
    ,u'唐':u'比亚迪'
    ,u'宋':u'比亚迪'
    ,u'秦':u'比亚迪'
    ,u'风神':u'东风'
    ,u'科沃滋':u'科沃兹'
    ,u'沃兰铎':u'沃兰多'
    ,u'克鲁兹':u'科鲁泽'
    ,u'科鲁兹':u'科鲁泽'
    ,u'克鲁泽':u'科鲁泽'
    ,u'迈瑞宝':u'迈锐宝'
    ,u'宝莱':u'宝来'
    ,u'福瑞斯':u'福睿斯'
    ,u'斯科达':u'斯柯达'
    ,u'凌度':u'凌渡'
    ,u'零度':u'凌渡'
    ,'CX4':u'马自达'
    ,u'标志':u'标致'
    ,u'帕斯特':u'帕萨特'
}

level_1_keyword = {
    u'二网':['2网','二网','二级','资源','汽贸','新元素','众合','毛豆','花生好车','蓝池','弹个车']
    ,u'资金问题':['预算','征信','贷款','按揭','信用','黑户','优惠力度','贵','价格','价钱','没钱','房','装修'
        ,'做生意','钱紧','经济压力','买不起','首付','优惠','资金','补贴','钱不到位']
    ,u'无购车资质':['没驾驶证','驾照','考车牌','驾驶本','驾驶证','18岁','十八岁','居住证','购车资质','户口','科目'
        ,'不符合条件','指标','暂住证','上牌','牌照']
    ,u'无购车需求':['不买车','不换车','有车','不想买','不买','不打算','不看车','不用','没需要','没看','不看','没关注'
        ,'不想了解','不需要','不关注','没咨询','没考虑','没购车意向','没意向','不会开车','没购车计划','已不准备'
        ,'不是自己','没计划','不考虑']
    ,u'暂不买车':['不着急','先开着','试乘','试驾','看看','随便看','看着玩','今年','明年','年底','近期','延后','下半年'
        ,'年后','暂时不考虑','等新款','怀孕']
    ,u'无法联系':['无人接','未接','不接','没接','拒接','关机','无法接','打不通','没打通','失联','无法打通','联系不上'
        ,'通话中','无法联系','正忙','未打通','无法拨通','语音信箱','打不进','没人接']
    ,u'购车偏好问题':['没看上','看不上','没相中','相不中','不好看','外观','颜色','空间','排量','配置','动力','不保值'
        ,'档次','知名度','油耗','内饰','噪音','隔音','变速箱','舒适度','不喜欢','耐看','质量']
    ,u'无现车':['停产','现车','没车','停售']
    ,u'竞品意向客户':['合资','进口','指定','日产','日系','不考虑名爵'
        ,'荣威','高尔夫','比亚迪','朗逸','宝来','奇瑞','吉利','途岳','奕歌','速腾','POLO','欧蓝德','探歌','捷途','马自达'
        ,'昂克赛拉','凌派','飞度','现代','缤智','凯美瑞','思域','猎豹','英朗','东风','雷凌','奇骏','铃木','锋范','三菱'
        ,'传奇','宝沃','劲客','名图','大众','艾瑞泽','丰田','领动','逸动','菲斯特','启辰','别克','雪佛兰','传祺','蓝鸟'
        ,'奔腾','斯柯达','瑞纳','智跑','福睿斯','探界者','广汽','尼桑','柯米克','福克斯','小鹏','长城','风骏','逍客'
        ,'菲斯塔','博瑞','星途','科沃兹','一汽','江淮','威朗','朗动','指南者','沃兰多','北汽','博越','凌渡','星越','悦动'
        ,'威驰','科鲁泽','起亚','君威','捷达','雷诺','致炫','大通','本田','宝骏','领克','中华','东南','阿特兹','海马'
        ,'帝豪','五菱','汉腾','杰德','长安','福特','迈锐宝','远景','明锐','菲亚特','众泰','瑞虎','昂科拉','哈弗','越野'
        ,'雪铁龙','途胜','骐达','通用','轩逸','卡罗拉','广本'
        ,'蔚来','奥迪','路虎','奔驰','宝马','吉普','途昂','帕萨特','JEEP','特斯拉','自由光','冠道','雅阁','探岳','奥德赛'
        ,'凯迪拉克','汉兰达','蒙迪欧','途观','英菲尼迪','皓影','天籁','柯迪亚克','极光','沃尔沃','捷豹','斯巴鲁','迈腾'
        ,'红旗','荣放','君越']
    ,u'家人朋友反对':['不同意','不赞同','不支持','家里','反对','不建议','媳妇不让']
    ,u'经销商地理位置不合适':['外地','远','回老家','在老家','过不来','异地','不来这边','不在这边','不会来','地区','就近'
        ,'当地','同城','外省','本地'] + list(pd.read_csv('support_data/location.txt',delimiter='\t')['location'])
}

level_2_dict = {
    u'无购车需求':
    {
        u'不换车':[u'不换车',u'有车']
        ,u'不买车':[u'不买车',u'不想买',u'不买',u'不打算',u'不看车',u'不用',u'没需要',u'没看',u'不看',u'没关注',u'不想了解',u'不需要',u'不关注'
                ,u'没咨询',u'没考虑',u'没购车意向',u'没意向',u'不会开车',u'没购车计划',u'已不准备',u'不是自己',u'没计划',u'不考虑']
    }
    ,u'竞品意向客户':
    {
        u'同级意向竞品':[u'荣威',u'高尔夫',u'比亚迪',u'朗逸',u'宝来',u'奇瑞',u'吉利',u'途岳',u'奕歌',u'速腾','POLO',u'欧蓝德',u'探歌',u'捷途'
                  ,u'马自达',u'昂克赛拉',u'凌派',u'飞度',u'现代',u'缤智',u'凯美瑞',u'思域',u'猎豹',u'英朗',u'东风',u'雷凌',u'奇骏',u'铃木'
                  ,u'锋范',u'三菱',u'传奇',u'宝沃',u'劲客',u'名图',u'大众',u'艾瑞泽',u'丰田',u'领动',u'逸动',u'菲斯特',u'启辰',u'别克'
                  ,u'雪佛兰',u'传祺',u'蓝鸟',u'奔腾',u'斯柯达',u'瑞纳',u'智跑',u'福睿斯',u'探界者',u'广汽',u'尼桑',u'柯米克',u'福克斯',u'小鹏'
                  ,u'长城',u'风骏',u'逍客',u'菲斯塔',u'博瑞',u'星途',u'科沃兹',u'一汽',u'江淮',u'威朗',u'朗动',u'指南者',u'沃兰多',u'北汽'
                  ,u'博越',u'凌渡',u'星越',u'悦动',u'威驰',u'科鲁泽',u'起亚',u'君威',u'捷达',u'雷诺',u'致炫',u'大通',u'本田',u'宝骏',u'领克'
                  ,u'中华',u'东南',u'阿特兹',u'海马',u'帝豪',u'五菱',u'汉腾',u'杰德',u'长安',u'福特',u'迈锐宝',u'远景',u'明锐',u'菲亚特'
                  ,u'众泰',u'瑞虎',u'昂科拉',u'哈弗',u'越野',u'雪铁龙',u'途胜',u'骐达',u'通用',u'轩逸',u'卡罗拉',u'广本']
        ,u'高级意向竞品':[u'蔚来',u'奥迪',u'路虎',u'奔驰',u'宝马',u'吉普',u'途昂',u'帕萨特','JEEP',u'特斯拉',u'自由光',u'冠道',u'雅阁',u'探岳'
                   ,u'奥德赛',u'凯迪拉克',u'汉兰达',u'蒙迪欧',u'途观',u'英菲尼迪',u'皓影',u'天籁',u'柯迪亚克',u'极光',u'沃尔沃',u'捷豹'
                   ,u'斯巴鲁',u'迈腾',u'红旗',u'荣放',u'君越']
    }
    ,u'信息被盗重复线索':
    {
        u'盗取信息':[u'不是本人',u'本人不符',u'信息不对',u'恶意留',u'假',u'被盗',u'盗号',u'盗用']
        ,u'推销':[u'推销',u'卖保险',u'做保险',u'保险公司']
        ,u'空号':[u'空号']
        ,u'错号':[u'错误',u'错号',u'号码错',u'有误',u'号码不存在',u'录错',u'建错',u'错卡',u'虚假线索',u'虚假号码',u'无效号码',u'无效信息']
        ,u'停机':[u'停机',u'停用',u'暂停服务',u'过期']
        ,u'误留':[u'留错',u'打错',u'点错',u'误点',u'乱点',u'误留']
    }
    ,u'暂不买车':
    {
        u'随便看看':[u'看看',u'随便看',u'看着玩']
        ,u'不着急':[u'不着急',u'先开着',u'暂时不考虑']
        ,u'等新款':[u'等新款']
    }
    ,u'经销商地理位置不合适':
    {
        u'客户更换地址':[u'就近',u'回老家',u'在老家',u'当地',u'同城',u'本地']
    }
    ,u'资金问题':
    {
        u'没预算':[u'钱不到位',u'预算',u'没钱',u'房',u'装修',u'做生意',u'钱紧',u'经济压力',u'买不起',u'资金']
        ,u'性价比低':[u'贵',u'价格',u'价钱',u'优惠',u'优惠力度',u'补贴']
        ,u'服务费高':[u'服务费']
        ,u'征信问题':[u'征信',u'贷款',u'按揭',u'信用',u'黑户']
    }
    ,u'二网':
    {
        u'二级经销商':[u'2网',u'二网',u'二级',u'资源',u'新元素',u'众合',u'毛豆',u'花生好车',u'蓝池',u'弹个车']
        ,u'汽贸城':[u'汽贸']
    }
    ,u'相关业务咨询':
    {
        u'售后':[u'售后',u'维修',u'修车',u'400',u'保养',u'客服',u'续保']
        ,u'退订':[u'退订金',u'退意向金',u'退车',u'退单',u'退订']
        ,u'寻找大客户经理':[u'租赁',u'网约车',u'大客户',u'客户经理',u'大经理',u'大电话']
    }
    ,u'购车偏好问题':
    {
        u'配置':[u'配置',u'空间']
        ,u'功能':[u'排量',u'油耗',u'噪音',u'隔音',u'变速箱']
        ,u'外观':[u'外观',u'颜色',u'内饰',u'耐看']
        ,u'动力':[u'动力']
    }
    ,u'无购车资质':
    {
        u'无驾照':[u'没驾驶证',u'驾照',u'考车牌',u'驾驶本',u'驾驶证',u'科目']
        ,u'无名额':[u'居住证',u'购车资质',u'户口',u'不符合条件',u'指标',u'暂住证']
        ,u'无车牌':[u'上牌',u'牌照']
    }
    ,u'无现车':
    {
        u'停产':[u'停产',u'停售']
    }
}


###########################################
#          Data Cleaning          #
##########################################
# extract chinese character
def extract_cn(content):
    content_str = ''
    for i in content:
        if i >= u'\u4e00' and i <= u'\u9fa5':
            content_str = content_str+i
    return content_str

# remove useless information
def remove_useless(string):
    if string is np.nan:
        val = ''
    elif len("".join(OrderedDict.fromkeys(string.decode('utf-8')))) == 1:
        val = ''
    elif "".join(OrderedDict.fromkeys(string.decode('utf-8'))) == '战败':
        val = ''
    elif "".join(OrderedDict.fromkeys(string.decode('utf-8'))) == '无效':
        val = ''
    else:
        val = string
    return val

# convert traditional character to simplified character
def Traditional2Simplified(sentence):
    sentence = Converter('zh-hans').convert(sentence)
    return sentence

# unify expression
def replace_word(string):
    segs = jieba.cut(string, cut_all=False)
    sent = '/'.join(segs).encode('utf-8').decode('utf-8')
    final_sentence = ''
    for word in sent.split('/'):
        if word in clean_word_dict:
            word = clean_word_dict[word]
            final_sentence += word
        else:
            final_sentence += word
    return final_sentence



###########################################
#            Reason Extraction            #
##########################################
def failed_deleted(string):
    # define keywords
    key_dict = {'信息被盗重复线索':['假','错误','被盗','盗号','盗用','空号','停机','停用','错号','留错','打错','点错','重复','推销','卖保险'
                             ,'做保险','误点','乱点','号码错','有误','号码不存在','暂停服务','过期','不是本人','录错','无效号码','无效信息'
                             ,'误留','灌水','瞎看','建错','虚假线索','错卡','虚假号码','重号','恶意留','本人不符','保险公司','信息不对']
                ,'刷单':['刷机','刷线索','刷试驾','刷试乘试驾','刷系','刷建卡','刷数据','刷单','刷的','刷电话','刷客流','刷卡率','补线索',
                  '补号码','做数据','拉数据','建卡率','报档案','涮','补单','冲线索','自建','冲单','虚建卡','做系统']
                ,'厂家测试抽查':['点检','厂家老师','抽查','测试','内部','自己人','员工','公司人员','工作人员','试验','同事','暗访','查岗'
                            ,'内职','标检','调试','实验','营销部']
                ,'拒绝沟通':['挂机','挂断','挂电话','直接挂','拒绝','挂线','拉黑','删','抗拒','就挂','挂掉','不讲话','接挂','切断','掐电'
                         ,'秒挂','不沟通','秒拒','屏蔽','拦截','闪挂','直挂','都挂']
                ,'相关业务咨询':['售后','维修','修车','退订金','退意向金','租赁','网约车','400','大客户','退车','保养','退单','客服','退订'
                           ,'客户经理','大经理','大电话','续保']
                ,'竞争对手探测':['二手网站','做二手车','同行','做黄牛','黄牛倒车','是黄牛','也是卖车','经销商','做车行','销售顾问','套价'
                           ,'炒车','竞争','倒车','探价','探子','销售员','也卖车']
                ,'已购车':{
                         'dealed':['成交','买别的','买其他','买二手','订别的','开票','买2手']
                        ,'verb':['买','定','订','购','提','交','签','选']
                        ,'status_1':['已','好','掉','完','的','过']
                        ,'status_2':['了']
                        ,'privative':['不','非']
                       }
                ,'需求不匹配':['皮卡','七座','7座','面包车','两厢','B级','拉货','微型','MPV','卡车']}
    
    punctuation = [",","，",".","。"," "," "]
    loc_pri = []
    loc_sta = []
    loc_punc = []
    for i in key_dict['已购车']['privative']:
        pri = string.decode('utf-8').find(i)
        loc_pri.append(pri)
    for j in key_dict['已购车']['status_2']:
        sta = string.decode('utf-8').find(j)
        loc_sta.append(sta)
    for k in punctuation:
        punc = string.decode('utf-8').find(k)
        loc_punc.append(punc)
   
    if any(keyword in string for keyword in key_dict['信息被盗重复线索']):   
        val = u'信息被盗重复线索'
    elif any(keyword in string for keyword in key_dict['刷单']):   
        val = u'刷单'
    elif any(keyword in string for keyword in key_dict['相关业务咨询']):
        val = u'相关业务咨询'
    elif any(keyword in string for keyword in key_dict['厂家测试抽查']):   
        val = u'厂家测试抽查'
    elif any(keyword in string for keyword in key_dict['已购车']['dealed']):
        val = u'已购车'
    elif (any(keyword in string for keyword in key_dict['已购车']['verb'])) & \
    (any(keyword in string for keyword in key_dict['已购车']['status_1'])):
        val = u'已购车'
    elif (any(keyword in string for keyword in key_dict['已购车']['verb'])) & \
    (any(keyword in string for keyword in key_dict['已购车']['status_2'])) & \
    (min([i if i >= 0 else np.nan for i in loc_pri]) is np.nan):
        val = u'已购车'
    elif (any(keyword in string for keyword in key_dict['已购车']['verb'])) & \
    (min([i if i >= 0 else np.nan for i in loc_pri]) > min([i if i >= 0 else np.nan for i in loc_sta])):
        val = u'已购车'
    elif (any(keyword in string for keyword in key_dict['已购车']['verb'])) & \
    (any(loc >= min([i if i >= 0 else np.nan for i in loc_pri]) for loc in [i if i >= 0 else np.nan for i in loc_punc])) & \
    (any(loc <= min([i if i >= 0 else np.nan for i in loc_sta]) for loc in [i if i >= 0 else np.nan for i in loc_punc])):
        val = u'已购车'
    elif any(keyword in string for keyword in key_dict['竞争对手探测']):
        val = u'竞争对手探测'
    elif any(keyword in string for keyword in key_dict['拒绝沟通']):
        val = u'拒绝沟通'
    elif any(keyword in string for keyword in key_dict['需求不匹配']):
        val = u'需求不匹配'
    else:
        val = np.nan        
    
    return val  


def fail_reason_group(string,reason_group):
    if any(keyword in string for keyword in level_1_keyword[reason_group]):
        val = reason_group
    else:
        val = np.nan
    return val

def convert_fail_group(df):
    fail_group = df[['fail_group']].dropna()
    fail_group = pd.DataFrame(fail_group.groupby(by = fail_group.index).apply(lambda x:','.join(x['fail_group'])),columns = ['fail_group'])
    def regroup(string):
        if string.find('信息被盗重复线索') != -1:
            val = u'信息被盗重复线索'
        elif string.find('刷单') != -1:
            val = u'刷单'
        elif string.find('相关业务咨询') != -1:
            val = u'相关业务咨询'
        elif string.find('厂家测试抽查') != -1:
            val = u'厂家测试抽查'
        elif string.find('已购车') != -1:
            val = u'已购车'
        elif string.find('竞争对手探测') != -1:
            val = u'竞争对手探测'
        elif string.find('拒绝沟通') != -1:
            val = u'拒绝沟通'
        elif string.find('需求不匹配') != -1:
            val = u'需求不匹配'
        else:
            val = np.nan
        return val
    fail_group.fail_group = fail_group.fail_group.apply(lambda x:regroup(x))
    return fail_group

def level_2(df,reason):
    for i in reason.keys():
        for j in reason[i].keys():
            logger.info('Processed Level II Failed Reason: %s',j)
            df[j] = np.nan
            df[j][(df.level_1.astype(str).str.decode('unicode_escape').str.contains(i)) & \
                  (df.d_fail_desc_list.astype(str).str.decode('unicode_escape').str.contains('|'.join(reason[i][j])))] = j
    return df

def cat_df(df,col_list):
    i = 1
    cat_df = df[col_list[0]]
    while i < len(col_list):
        cat_df = cat_df.str.cat(df[col_list[i]],sep=',',na_rep='-')
        i = i + 1
    cat_df = cat_df.apply(lambda x: [i for i in x.split(',') if i != "-"])
    return cat_df



#####################################
#        Running            #
####################################
def running():
    # fetch data
    failed_info = hc.sql(
	'''
        SELECT mobile,d_fail_desc_list,d_deal_flag FROM marketing_modeling.app_big_wide_info
        WHERE mobile REGEXP "^[1][3-9][0-9]{9}$" 
        AND pt = %s
	'''%(sys.argv[1])
	).toPandas()
	#'''%('20200716')).toPandas()
    fail = failed_info.dropna()
    fail = fail[[len(i) != 0 for i in fail['d_fail_desc_list']]]
    logger.info('Load %s Data!', str(fail.shape))
    fail_reason = pd.DataFrame({'mobile': fail.mobile.repeat(fail.d_fail_desc_list.str.len()),\
                                'fail_desc': np.concatenate(fail.d_fail_desc_list.values)})
    fail_reason = fail_reason.set_index('mobile')
    logger.info('Fetch Data End!')

    # Data cleaning
    ## extract chinese character and then move descriptions that only contains alpha, numbers and punctuations
    fail_reason['fail_char'] = fail_reason['fail_desc'].astype(str).apply(lambda x: extract_cn(x.decode('utf-8')))
    logger.info('Exact Chinese Characters End!')

    fail_reason['fail_char'] = fail_reason['fail_char'].astype(str).apply(lambda x: remove_useless(x.decode('utf-8')))
    fail_reason = fail_reason[fail_reason['fail_char'] != '']
    logger.info('Remove Useless words End!')

    ## convert traditional character to simplified character
    fail_reason['fail_reason'] = fail_reason['fail_desc'].apply(lambda x: Traditional2Simplified(x.decode('utf-8')))
    logger.info('Convert to Simplified Characters End!')

    ## convert lowcase to uppercase
    fail_reason['fail_reason'] = [str(i).upper() for i in fail_reason['fail_reason']]
    logger.info('Convert to Uppercase End!')

    ## unify expression
    fail_reason['fail_reason'] = fail_reason['fail_reason'].apply(lambda x: replace_word(x.decode('utf-8')))
    logger.info('Unify Expressions End!')
    logger.info('Data Cleaning End!')


    # Level I reason extraction
    ## generate level I reason
    fail_reason['fail_group'] = fail_reason['fail_reason'].apply(lambda x: failed_deleted(x))
    logger.info('Generate All Level I Deleted Reasons End!')

    for i in level_1_keyword.keys():
        logger.info('Processe %s', i)
        fail_reason[i] = fail_reason['fail_reason'].apply(lambda x: fail_reason_group(x, i))
    logger.info('Generate All Level I Reasons End!')

    ## group together by each customers
    fail_group = convert_fail_group(fail_reason) # keep only one deleted reason for each customer
    for i in level_1_keyword.keys():
        logger.info('Group Level I Reason: %s', i)
        df = fail_reason[[i]].where(fail_reason[[i]].isnull(), 1).fillna(0).groupby(by=fail_reason.index).sum()
        df = df[[i]].where(df[i] == 0, i)
        df = df[[i]].where(df[i] != 0, np.nan)
        fail = fail.merge(df, left_on='mobile', right_on=df.index, how='left')
    fail = fail.merge(fail_group, left_on='mobile', right_on=fail_group.index, how='left')
    fail['fail_group'][fail['d_deal_flag'] == 1] = u'已购车'
    ## merge together
    fail['level_1'] = cat_df(fail,['fail_group'] + level_1_keyword.keys())
    logger.info('Level I Reasons Processed End!')

    # Level II reason extraction
    ## generate level II reason
    fail = level_2(fail, level_2_dict)
    # 已购竞品
    fail[u'已购竞品'] = np.nan
    fail[u'已购竞品'][(fail.level_1.astype(str).str.decode('unicode_escape').str.contains('已购车'.decode('utf-8'))) & \
                  (fail.d_fail_desc_list.astype(str).str.decode('unicode_escape')\
                   .str.contains('|'.join(level_2_dict[u'竞品意向客户'][u'同级意向竞品']+\
                                          level_2_dict[u'竞品意向客户'][u'高级意向竞品'])))] = u'已购竞品'
    # 经销商下发地址错误
    fail[u'经销商下发地址错误'] = np.nan
    fail[u'经销商下发地址错误'][(fail['level_1'].astype(str).str.decode('unicode_escape').str.contains('经销商地理位置不合适'.decode('utf-8'))) & \
                      (fail[u'客户更换地址'].isna())] = u'经销商下发地址错误'
    # 已购名爵
    fail[u'已购名爵'] = np.nan
    fail[u'已购名爵'][fail['d_deal_flag'] == 1] = u'已购名爵'

    # get all second level reasons from dictionary
    key = []
    for k, v in level_2_dict.items():
        if isinstance(v, dict):
            val = v.keys()
            key += val
    level_2_list = key + [u'已购竞品',u'经销商下发地址错误',u'已购名爵']

    ## merge together
    fail['level_2'] = cat_df(fail, level_2_list)
    logger.info('Level II Reasons Processed End!')

    reason_result = fail[['mobile','level_1','level_2']]

    return reason_result

###########################################
#      Generate and Save Result      #
##########################################
spark_session = SparkSession.builder.enableHiveSupport().appName("M_Model").config("spark.driver.memory","30g").getOrCreate()
hc = HiveContext(spark_session.sparkContext)
hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hc.setConf("tez.queue.name", "malg")
result = running()
print(result.head())
fail_reason_result = hc.createDataFrame(result)
fail_reason_result.show(5)
fail_reason_result.write.insertInto("marketing_modeling.app_failed_reason",overwrite=True)