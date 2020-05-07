import json
import codecs
import datetime
import pandas as pd
from fuzzywuzzy import fuzz
from LTPFunc import LTPFunction
import utils
from tqdm import tqdm
from find_es_vps import find_viewpoints_by_news_id

'''
# 将json数据转换为csv格式, 并根据文章内容进行去重
dataList = []   # 数据列表
text_list = []  # 用于存储比较文章内容
sim_score = 50  # 设置相似度的阈值
with codecs.open("data/南海航行自由.txt",'r','utf-8') as f:
    for line in f.readlines():
        sim_flag = False    # 判断相似
        result = json.loads(line) # dict格式
        try:
            time = result['time'].replace(' CST','')
            result['time'] = str(datetime.datetime.strptime(time,'%a %b %d %H:%M:%S %Y')) # 将time格式化, 详细信息查阅 http://blog.chinaunix.net/uid-27659438-id-3350887.html
            result['text'] = result['text'].replace('\n','')
            for t in text_list:
                score = fuzz.ratio(result['text'], t)
                if score > sim_score:   # 计算文本相似度
                    sim_flag = True
                    print(score, ":", result['text'], "   ", t)
                    break
            if sim_flag:
                continue
            else:
                text_list.append(result['text'])
                dataList.append(result)
        except:
            print(result)

data_df = pd.DataFrame(dataList)
data_df['time'] = pd.to_datetime(data_df['time'])
data_df.sort_values('time', inplace=True)
data_df.to_csv("data/南海自由航行_去重.csv", index=False)
'''


'''
# 根据内容间的编辑距离去重
title_list = []
with codecs.open("data/南海航行自由.txt",'r','utf-8') as f:
    for line in f.readlines():
        sim_flag = False
        result = json.loads(line) # dict格式
        for t in title_list:
            score = fuzz.ratio(result['text'], t)
            if score > 50:   # 计算文本相似度
                sim_flag = True
                print(score, ":", result['text'], "   ", t)
                break
        if sim_flag:
            continue
        else:
            title_list.append(result['text'])

print(len(title_list))
'''

'''
# 根据关键词检索数据
df = pd.read_csv("data/南海_2018-01-01_2020-02-10.csv")
df = df.dropna(subset=["content", "title"]) # 删除content, title中值为Nan的行
result = df[df['original_text'].str.contains('海军|南海|航行')] # https://blog.csdn.net/htbeker/article/details/79645651
result.to_csv("result/南海航行观点筛选.csv", index=False)
# print(df['news_id'])
'''

'''
# 根据news_id检索数据库中的观点
theme_name = "南海自由航行"
news_df = pd.read_csv("data/" + theme_name + "_news.csv")
news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
vps_list = find_viewpoints_by_news_id(news_id, size=3000)   # 从观点库中根据news_id查找对应的观点
vps_df = pd.DataFrame(vps_list)
vps_df.to_csv("data/" + theme_name + "_views.csv", index=False) # 将观点数据存入文件中
'''

'''
# 根据新闻评论计算新闻影响力指数
theme_name = "南海自由航行"
news_df = pd.read_csv("data/" + theme_name + "_news.csv")
result = utils.news_comment_deal(news_df)
with codecs.open("result/news_influence.json", "w", "utf-8") as wf:
    json.dump(result, wf, indent=4)
'''

# 根据bd56部署的知识图谱查询 专家人名信息/机构信息, 对观点数据进行补全
import requests
# result = requests.get("http://10.1.1.56:9000/eav?entity=李华敏&attribute=国籍")

baseurl = "http://10.1.1.56:9000/eav?"
views_df = pd.read_csv("data/南海自由航行_views.csv")
per_country_dict = {}
total_count = 0
non_count = 0 # 没有检索出来的人名
for per in views_df['person_name']:
    if per not in per_country_dict:
        total_count += 1
        result = requests.get(baseurl + "entity=" + per + "&attribute=国籍")
        if result.text == "":
            non_count += 1
        else:
            per_country_dict[per] = result.text

with codecs.open("result/per_country.txt","w","utf-8") as wf:
    for key, value in per_country_dict.items():
        wf.write(key + ": " + value + "\n")

