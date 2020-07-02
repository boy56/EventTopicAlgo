# -*- coding: UTF-8 -*-
'''
对新闻数据进行预处理, 并依据国家关键词、事件关键词制作国家、事件分类标签
'''

import datetime
import pandas as pd
from tqdm import tqdm

from ClassifyFunc import ClassifyFunc


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
