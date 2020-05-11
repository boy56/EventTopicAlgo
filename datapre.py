# -*- coding: UTF-8 -*-
'''
1、根据新增的新闻列表从实验室观点系统中获取观点列表
2、对新闻数据进行预处理, 并依据国家关键词、事件关键词制作国家、事件分类标签
3、获取专家对应的国家, 更新之前已经存储的 {专家:国家} 字典(存在则获取以往国家字段，不存在则根据算法补全)
注: 该脚本需要运行在实验室(ACT)内网环境
'''

import requests
import json
from elasticsearch_dsl import Document,Q
from elasticsearch import Elasticsearch
import math
import pickle
import pandas as pd

import datetime
from tqdm import tqdm
from ClassifyFunc import ClassifyFunc
import codecs

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

# 根据新闻信息和对应的观点信息计算新闻的正负向情感数值以及影响力指数
def news_deal(theme_name, news_df, views_df):
    # 根据dict获取关键词字典并进行国家、事件分类
    classifyFunc = ClassifyFunc(theme=theme_name)
    title_country_dict = {} # title/country 字典
    title_content_dict = {} # title/content 字典
    # 根据新闻信息和对应的观点信息计算新闻的正负向情感数值以及影响力指数
    newsid_pos_segment = {}
    newsid_neg_segment = {}
    newsid_influence = {}

    for i in range(0, len(news_df)):
        pos_num = 0 # 专家观点情绪为正的数量
        neg_num = 0 # 专家观点情绪为负的数量
        influence = 0 # 每多一条专家观点, 新闻的影响力就+1

        row = news_df.iloc[i]
        n_id = row['news_id']
        title = row['title']
        title_country_dict[title] = classifyFunc.classify_title(title, dict_type=1) # 计算新闻国家标签
        title_content_dict[title] = classifyFunc.classify_title(title, dict_type=0) # 计算新闻内容标签

        new_views = views_df[views_df['news_id'] == n_id]
        for v_s in new_views['sentiment']:
            # 判断专家观点的情绪
            if v_s > 0.6:
                pos_num += 1
            else:
                neg_num += 1
            influence += 1 # 有一个专家观点则增加1的新闻的影响力指数, 后续可以根据专家的权重来更改
        
        # 对新闻的正负向指数进行归一化
        if pos_num + neg_num == 0:
            newsid_pos_segment[n_id] = 0
            newsid_neg_segment[n_id] = 0
        else:
            newsid_pos_segment[n_id] = float("%.2f" % (pos_num/(pos_num + neg_num)))
            newsid_neg_segment[n_id] = float("%.2f" % (neg_num/(pos_num + neg_num)))
        newsid_influence[n_id] = influence
    
    # 将国家标签、内容标签添加到数据中
    news_df['country_label']=news_df['title'].map(title_country_dict)
    news_df['content_label']=news_df['title'].map(title_content_dict)
    # 将正负向情感指数、影响力指数、国家标签、内容标签添加到数据中
    news_df['positive'] = news_df['news_id'].map(newsid_pos_segment)
    news_df['negative'] = news_df['news_id'].map(newsid_neg_segment)
    news_df['influence'] = news_df['news_id'].map(newsid_influence)

    news_df.to_csv("data/" + theme_name + "_news_newdata.csv",index=False)

# 根据专家、机构获取观点对应的国籍
def views_deal(theme_name, views_df):
    # 加载之前已经存在的字典
    pkl_rf = open('dict/per_country.pkl','rb')
    per_country_dict = pickle.load(pkl_rf)
    
    # 加载中文国家名称转换信息
    with codecs.open("dict/zhcountry_convert.json",'r','utf-8') as jf:
        zhcountry_convert_dict = json.load(jf)

    # 加载echarts世界地图国家中文名
    pkl_rf = open('dict/echarts_zhcountry_set.pkl','rb')
    zhcountry_set = pickle.load(pkl_rf)

    #加载之前存储的{专家：国家}字典
    pkl_rf = open('dict/per_country.pkl','rb')
    per_country_dict = pickle.load(pkl_rf)

    # 为每条观点获取国家属性
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

    # print("补全专家人数:" + str(org2per_count))
    views_df['country'] = view_country_list # 新增一列国家

    # 统计该专题下的{国家-观点数量分布}
    country_view_dict = {}
    for country in views_df["country"]:
        if country in country_view_dict:
            country_view_dict[country] += 1
        else:
            country_view_dict[country] = 1

    # 存储不同专题的国家-观点数量信息,用于前端直接读取
    pklf = open("dict/" + theme_name+ "_countryviews_dict.pkl","wb") 
    pickle.dump(country_view_dict, pklf)

    # 保存{人名:国家}字典
    pklf = open("dict/per_country.pkl","wb") 
    pickle.dump(per_country_dict, pklf) 

    views_df.to_csv("data/" + theme_name + "_views_newdata.csv", index=False) # 将增加国家数据的观点数据存入文件中


if __name__ == "__main__":
    theme_name = "南海"
    news_df = pd.read_csv("data/" + theme_name + "_news.csv")
    news_df = news_df.dropna(subset=["content", "title"]) # 删除content, title中值为Nan的行

    # 根据news_id检索数据库中的观点
    news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
    vps_list = find_viewpoints_by_news_id(news_id)   # 从观点库中根据news_id查找对应的观点
    views_df = pd.DataFrame(vps_list)
    # views_df = pd.read_csv("data/南海_views.csv")
    
    # 对新闻数据新增标签
    # news_deal(theme_name, news_df, views_df)

    # 对观点数据新增国家标签
    views_deal(theme_name, views_df)