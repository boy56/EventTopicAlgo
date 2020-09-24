# -*- coding: UTF-8 -*-
'''
国家字段补全函数
1、根据人名查找Ownthink库获取国家属性
2、根据人名、机构名获取国家属性
'''

import codecs
import pickle
import json
from models import NewsInfo, ViewsInfo, mysql_db, OtherNewsInfo
import requests
import pandas as pd
from tqdm import tqdm

class PerCountryDeal:
    def __init__(self):
        # 加载之前已经存在的人名-国家字典
        pkl_rf = open('dict/per_country.pkl','rb')
        self.per_country_dict = pickle.load(pkl_rf)
    
        # 加载中文国家名称转换信息
        with codecs.open("dict/zhcountry_convert.json",'r','utf-8') as jf:
            self.zhcountry_convert_dict = json.load(jf)

        # 加载echarts世界地图国家中文名
        pkl_rf = open('dict/echarts_zhcountry_set.pkl','rb')
        self.zhcountry_set = pickle.load(pkl_rf)

        #加载之前存储的{专家：国家}字典
        pkl_rf = open('dict/per_country.pkl','rb')
        self.per_country_dict = pickle.load(pkl_rf)

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(PerCountryDeal, cls).__new__(cls)
        return cls.instance


    # 根据ownthink知识图谱查找人名所对应的国籍
    def findCountryFromOwnthink(self, entity):
        result = requests.get("http://10.1.1.56:9000/eav?entity=" + entity + "&attribute=国籍")
        r = json.loads(result.text)
        if r != []:
            return r[0]
        else:
            # print("????????????????????")
            # print(len(entity))
            result = requests.get("https://api.ownthink.com/kg/knowledge?entity=" + entity)
            # print(result.text)
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

    # 根据人名、职位获取对应的国家信息
    def find_country(self, per: str, pos: str) -> str:
        per_country = "N"

        # 如果该专家之前已经处理过
        if isinstance(per, str) and per in self.per_country_dict: # person不为空
            if self.per_country_dict[per] is not "N":    # 该专家的国家名称不为N 
                per_country = self.per_country_dict[per]   
                
                # 更新{人名:国家}字典
                # pklf = open("dict/per_country.pkl","wb") 
                # pickle.dump(self.per_country_dict, pklf) 
                return per_country

        # 先判断org中是否包含set中的国家
        # print(self.zhcountry_set)
        for con in self.zhcountry_set:
            if con in pos:
                per_country = con
                break
        
        # 如果在org中找到了符合要求的则进行存储并continue
        if per_country is not "N":
            if isinstance(per, str):
                self.per_country_dict[per] = per_country # 根据org字段补全专家国籍
            
            # 更新{人名:国家}字典
            # pklf = open("dict/per_country.pkl","wb") 
            # pickle.dump(self.per_country_dict, pklf) 
            return per_country

        # 根据per来查找知识图谱中的信息, 需要在ACT内网环境下执行
        if isinstance(per, str) and len(per) > 0: # 如果per字段不为空
            country = self.findCountryFromOwnthink(per)
            # 在进行国家对比的时候先进行转换
            if country in self.zhcountry_convert_dict:
                country = self.zhcountry_convert_dict[country]
            # 如果该国家在echarts中的中文国家字典中
            if country in self.zhcountry_set:
                per_country = country
            self.per_country_dict[per] = per_country

        
        # 更新{人名:国家}字典
        # pklf = open("dict/per_country.pkl","wb") 
        # pickle.dump(self.per_country_dict, pklf) 
        return per_country

if __name__ == '__main__':
    
    PerCountryDealFunc = PerCountryDeal()
    '''
    per = "马英九"
    pos = ""
    print(PerCountryDealFunc.find_country(per, pos))
    '''
    
    # 更新views_newdata.csv中的国家字段
    theme_name = "朝核"
    date_str = '202007'
    views_df = pd.read_csv("data/" + theme_name + "_" + date_str + "_views_newdata.csv")
    change_count = 0
    with codecs.open('result/PerCountryDealChangeLog.txt','w','utf-8') as wf:
        for i in tqdm(range(0, len(views_df))):
            row = views_df.iloc[i]
            per = row['person_name']
            org = str(row['org_name']) + str(row['pos'])
            old_country = row['country']
            new_country = PerCountryDealFunc.find_country(per, org)

            if new_country != old_country:
                views_df.loc[i, 'country'] = new_country
                wf.write("per: " + str(per) + "\n")
                wf.write("org: " + str(org) + "\n")
                wf.write("old_country: " + str(old_country) + "\n")
                wf.write("new_country: " + str(new_country) + "\n")
                wf.write("\n")
                change_count += 1
        wf.write(change_count)
    views_df.to_csv("data/" + theme_name + "_" + date_str + "_views_newdata_pro.csv", index=False) # 将增加国家数据的观点数据存入文件中

    '''
    # 更新数据库中的country字段
    views = ViewsInfo.select()
    change_count = 0
    for v in tqdm(views):
        per_country = PerCountryDealFunc.find_country(v.personname, str(v.orgname) + str(v.pos))
        if per_country == "N":
            per_country = ""    
        if v.country != per_country:
            v.country = per_country
            v.save()
            change_count += 1
    print(change_count)
    '''