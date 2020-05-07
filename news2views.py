# -*- coding: UTF-8 -*-
'''
1.对新闻数据进行预处理, 并依据国家关键词、事件关键词制作国家、事件分类标签
2.根据新闻数据从实验室观点系统中获取对应的观点数据
'''

import datetime
import pandas as pd
from tqdm import tqdm

from ClassifyFunc import ClassifyFunc
from find_es_vps import find_viewpoints_by_news_id


theme_name = "朝核"
news_df = pd.read_csv("data/" + theme_name + "_news.csv")
news_df = news_df.dropna(subset=["content", "title"]) # 删除content, title中值为Nan的行


# 根据dict获取关键词字典并进行国家、事件分类
classifyFunc = ClassifyFunc(theme=theme_name)
title_country_dict = {} # title/country 字典
title_content_dict = {} # title/content 字典
for title in tqdm(news_df['title']):
    title_country_dict[title] = classifyFunc.classify_title(title, dict_type=1)
    title_content_dict[title] = classifyFunc.classify_title(title, dict_type=0)

# 将国家标签、内容标签添加到数据中
news_df['country_label']=news_df['title'].map(title_country_dict)
news_df['content_label']=news_df['title'].map(title_content_dict)
news_df.to_csv("data/" + theme_name + "_news_newlabel.csv",index=False)

'''
# 根据news_id检索数据库中的观点
news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
vps_list = find_viewpoints_by_news_id(news_id)   # 从观点库中根据news_id查找对应的观点
vps_df = pd.DataFrame(vps_list)

# 根据bd56部署的知识图谱查询 专家人名信息/机构信息, 对观点数据进行补全
import requests

# result = requests.get("http://10.1.1.56:9000/eav?entity=李华敏&attribute=国籍")
baseurl = "http://10.1.1.56:9000/eav?"

print(result.text)
vps_df.to_csv("data/" + theme_name + "_views.csv", index=False) # 将观点数据存入文件中
'''