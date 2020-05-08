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
        country = [i[1] if i[0] == "国籍" else "" for i in json.loads(result.text)["data"]["avp"]]
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
    
    '''
    # 根据news_id检索数据库中的观点
    news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
    vps_list = find_viewpoints_by_news_id(news_id)   # 从观点库中根据news_id查找对应的观点
    vps_df = pd.DataFrame(vps_list)

    # views_df = views_df.dropna(subset=["person_name"])
    
    
    # views中per为空的处理

    for per in tqdm(views_df['person_name']):
        if per not in per_country_dict:
            result = requests.get(baseurl + "entity=" + per + "&attribute=国籍")
            countrys = json.loads(result.text)
            if len(countrys) == 0:
                per_country_dict[per] = 'N'
            else:
                per_country_dict[per] = countrys[0]
    


    # 保存{人名:国家} 字典
    pkl_wf = open("dict/per_country.pkl","w") 
    pickle.dump(per_country_dict, pkl_wf) 


    # 根据专家名字增加国家字段
    vps_df.to_csv("data/" + theme_name + "_views.csv", index=False) # 将观点数据存入文件中
    '''