# -*- coding: UTF-8 -*-
'''
1、根据新增的新闻列表从实验室观点系统中获取观点列表
2、获取专家对应的国家, 更新之前已经存储的 {专家:国家} 字典(存在则获取以往国家字段，不存在则根据算法补全)
注: 该脚本需要运行在实验室(ACT)内网环境
'''

import requests
import json
from elasticsearch_dsl import Document,Q
from elasticsearch import Elasticsearch
import math
import pickle
import pandas as pd

ES_IP = '10.1.1.36'
ES_PORT = 9200
ES_IP_PORT = '%s:%d' % (ES_IP, ES_PORT)

class Doc(Document):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        return

    def set_from_dict(self, attr_dict):
        for key, value in attr_dict.items():
            setattr(self, key, value)
        return self


class ViewPoint(Doc):
    class Meta:
        doc_type = "msg"
        index = "viewpoint2"


def query_vp_by_news(news_ids):
        if not news_ids:
            return Q()
        return Q("terms", news_id=news_ids)

# 根据新闻id获取对应的观点数据, 返回list<dict>
def find_viewpoints_by_news_id(news_ids=None,size=3000):
    es_client = Elasticsearch(ES_IP_PORT, timeout=600)
    vps_list = []
    slice_size = 500
    nslices = math.floor(len(news_ids) / slice_size)
    
    # 分批次查询
    for i in range(0, nslices):
        tmp_id = news_ids[i*slice_size:(i+1)*slice_size]    # 切片查询   
        q = query_vp_by_news(tmp_id)
        vps = ViewPoint.search().using(es_client).query(q).extra(size=size).execute()   # 输出为elasticsearch_dsl.response.Response对象
        for v in vps:
            # print(v.__dict__['_d_'])
            vps_list.append(v.__dict__['_d_'])
        # print(i," ", len(vps_list))
    # 将[nslices*slice_size:]进行处理
    tmp_id = news_ids[nslices*slice_size:]    # 切片查询   
    q = query_vp_by_news(tmp_id)
    vps = ViewPoint.search().using(es_client).query(q).extra(size=size).execute()   # 输出为elasticsearch_dsl.response.Response对象
    for v in vps:
        # print(v.__dict__['_d_'])
        vps_list.append(v.__dict__['_d_'])

    return vps_list

# 根据文本内容获取观点list
def find_viewpoints_by_content(news_list):
    # 根据文本内容得到对应观点信息的API, 实验室内部本地访问接口
    url = 'http://10.1.1.56:8800/news_to_vp_api/'
    news_list = ['中央军委主席习近平认为这次阅兵十分成功。']
    str1 = json.dumps(news_list,ensure_ascii=False)
    data = {'news_list':str1}

    response = requests.post(url,data=data)
    print(response.text)
    return response.text


# 根据ownthink知识图谱查找人名所对应的国籍
def findCountry(entity):
    result = requests.get("http://10.1.1.56:9000/eav?entity=" + entity + "&attribute=国籍")
    r = json.loads(result.text)
    if r != []:
        return r[0]
    else:
        result = requests.get("https://api.ownthink.com/kg/knowledge?entity=" + entity)
        result = json.loads(result.text)
        if 'avp' not in result['data']: return 'N'
        country = [i[1] if i[0] == "国籍" else "" for i in result["data"]["avp"]]
        r = []
        for i in country:
            if i != "":
                r.append(i)
        if r != []:
            return r[0]
        else:
            return "N"




if __name__ == "__main__":
    theme_name = "朝核"
    news_df = pd.read_csv("data/" + theme_name + "_news_newlabel.csv")

    # 加载之前已经存在的字典
    pkl_rf = open('dict/per_country.pkl','rb')
    per_country_dict = pickle.load(pkl_rf)
    print(type(per_country_dict))

    for key, value in per_country_dict.items():
        print(key + ": " + value)
        break
    
    # 根据news_id检索数据库中的观点
    news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
    vps_list = find_viewpoints_by_news_id(news_id)   # 从观点库中根据news_id查找对应的观点
    views_df = pd.DataFrame(vps_list)

    # 加载中文国家名称转换信息
    with codecs.open("dict/zhcountry_convert.json",'r','utf-8') as jf:
        zhcountry_convert_dict = json.load(jf)

    # 加载echarts世界地图国家中文名
    pkl_rf = open('dict/echarts_zhcountry_set.pkl','rb')
    zhcountry_set = pickle.load(pkl_rf)

    #加载之前存储的{专家：国家}字典
    pkl_rf = open('dict/per_country.pkl','rb')
    per_country_dict = pickle.load(pkl_rf)

    org2per_count = 0
    view_country_list = []
    for i in range(0, len(views_df)):
        row = views_df.iloc[i]
        per = row['person_name']
        org = str(row['org_name']) + str(row['pos'])
        # print(org)
        per_country = "N"
        
        # 如果该专家之前已经处理过
        if per in per_country_dict:
            if per_country_dict[per] is not "N":    # 该专家的国家名称不为N 
                # views_df.iloc[i]['country'] = per_country_dict[per] # 获取专家所在的国家
                view_country_list.append(per_country_dict[per])
                continue # 该专家已经存在库中则进行跳过

        # 先判断org中是否包含set中的国家
        for con in zhcountry_set:
            if con in org:
                per_country = con
                break
        
        # 如果在org中找到了符合要求的则进行存储并continue
        if per_country is not "N":
            if isinstance(per, str):
                org2per_count += 1 
                per_country_dict[per] = per_country # 根据org字段补全专家国籍
            # row['country'] = per_country
            view_country_list.append(per_country)
            continue

        # 根据per来查找知识图谱中的信息
        if isinstance(per, str): # 如果per字段不为空
            country = findCountry(per)
            # 在进行国家对比的时候先进行转换
            if country in zhcountry_convert_dict:
                country = zhcountry_convert_dict[country]
            # 如果该国家在echarts中的中文国家字典中
            if country in zhcountry_set:
                per_country = country
            per_country_dict[per] = per_country

        # row['country'] = per_country
        view_country_list.append(per_country)

    print("补全专家人数:" + str(org2per_count))
    views_df['country'] = view_country_list # 新增一列国家

    # 统计该专题下的{国家-观点数量分布}
    country_view_dict = {}
    for country in views_df["country"]:
        if country in country_view_dict:
            country_view_dict[country] += 1
        else:
            country_view_dict[country] = 1

    # 存储不同专题的国家-观点数量信息
    pklf = open("dict/" + theme_name+ "_countryviews_dict.pkl","wb") 
    pickle.dump(country_view_dict, pklf)

    # 保存{人名:国家}字典
    pklf = open("dict/per_country.pkl","wb") 
    pickle.dump(per_country_dict, pklf) 

    views_df.to_csv("data/" + theme_name + "_views.csv", index=False) # 将增加国家数据的观点数据存入文件中