# -*- coding: UTF-8 -*-
# 将数据依赖models中的样式导入mysql中

import pandas as pd
from utils import clean_zh_text
from datetime import datetime
from sqlalchemy import create_engine
from models import NewsInfo, ViewsInfo,mysql_db
import math

# 新闻csv导入mysql, 默认文件类型为从Hbase导出的数据
def newscsvtosql(path, theme, datatype=1):
    # 从csv文件读入数据
    df = pd.read_csv(path)
    df['time'] = pd.to_datetime(df['time'])
    df = df.fillna('')  # 填充NA数据
    # print(df.shape)
    # 遍历读取处理
    news_data = []
    for index, row in df.iterrows():
        tmp = {}
        tmp['newsid'] = row['news_id']
        tmp['title'] = clean_zh_text(row['title'], 2)
        tmp['time'] = datetime.strftime(row['time'],'%Y-%m-%d %H:%M:%S')    # 格式化时间字符串
        tmp['content'] = clean_zh_text(row['content'])  # 清洗正文内容
        tmp['url'] = row['url']
        tmp['customer'] = row['customer']
        # tmp['emotion'] = row['emotion']
        # tmp['entities'] = row['entities']
        # tmp['keyword'] = row['keyword']
        tmp['location'] = row['location']
        # tmp['pageview'] = row['pageview']
        # tmp['userview'] = row['userview']
        # tmp['words'] = row['words']

        tmp['theme_label'] = theme
        tmp['content_label'] = row['content_label']
        tmp['country_label'] = row['country_label']
        tmp['positive'] = row['positive']
        tmp['negative'] = row['negative']
        tmp['influence'] = row['influence']

        news_data.append(tmp)

    # print(len(news_data))
    # write data to mysql
    mysql_db.connect()
    
    # 插入新闻数据
    if not NewsInfo.table_exists(): # 如果表不存在则创建
        NewsInfo.create_table()
    # else: # bug调好后注释掉, 改为增量
    #    NewsInfo.delete().execute() # 每次重新更新之前清空数据表
    
    # 根据切片分批次插入
    slice_size = 300    # 切片大小
    nslices = math.floor(len(news_data) / slice_size)
    
    for i in range(0, nslices):
        with mysql_db.atomic():
            NewsInfo.insert_many(news_data[i * slice_size: (i + 1) * slice_size]).execute() # 批量插入
        # print(i)
    # 插入最后一个切片的数据
    with mysql_db.atomic():
        NewsInfo.insert_many(news_data[nslices*slice_size:]).execute() # 批量插入
    # print(nslices)
    mysql_db.close()

# 观点csv导入mysql, 默认文件类型为经过vps查询得到的观点数据列表
def viewscsvtosql(path, datatype=1):
     # 从csv文件读入数据
    df = pd.read_csv(path)
    df['time'] = pd.to_datetime(df['time'])
    df = df.fillna('')  # 填充NA数据

    # 遍历读取处理
    news_data = []
    for index, row in df.iterrows():
        tmp = {}
        tmp['personname'] = row['person_name']
        if row['country'] == 'N': 
            tmp['country'] = ""
        else: 
            tmp['country'] = row['country']
        tmp['orgname'] = row['org_name']
        tmp['pos'] = row['pos']
        tmp['verb'] = row['verb']
        tmp['viewpoint'] = row['viewpoint']
        tmp['newsid'] = row['news_id']
        tmp['sentiment'] = row['sentiment']
        tmp['original_text'] = row['original_text']
        tmp['time'] = datetime.strftime(row['time'],'%Y-%m-%d %H:%M:%S')    # 格式化时间字符串

        news_data.append(tmp)

    # write data to mysql
    mysql_db.connect()
    
    # 插入新闻数据
    if not ViewsInfo.table_exists(): # 如果表不存在则创建
        ViewsInfo.create_table()
    # else: # bug调好后注释掉, 改为增量
    #     ViewsInfo.delete().execute() # 每次重新更新之前清空数据表
    
    # 根据切片分批次插入
    slice_size = 300    # 切片大小
    nslices = math.floor(len(news_data) / slice_size)
    for i in range(0, nslices):
        with mysql_db.atomic():
            ViewsInfo.insert_many(news_data[i * slice_size: (i + 1) * slice_size]).execute() # 批量插入

    # 插入最后一个切片的数据
    with mysql_db.atomic():
        ViewsInfo.insert_many(news_data[nslices*slice_size:]).execute() # 批量插入

    mysql_db.close()
    return 

# 数据库建库建表
if __name__ == "__main__":
    theme_name = "朝核"
    date_str = '202007'
    newscsvtosql("data/" + theme_name + "_" + date_str + "_news_newdata.csv",theme_name)
    viewscsvtosql("data/" + theme_name + "_" + date_str + "_views_newdata.csv")

   
