import json
import codecs
import datetime
import pandas as pd
from fuzzywuzzy import fuzz
from LTPFunc import LTPFunction
from utils import clean_zh_text
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
# 依据LTP工具提取人物、地点、机构
person_set = set()
location_set = set()
organization_set = set()
result_list = []
ltpFunc = LTPFunction()

df = pd.read_csv("data/南海自由航行_去重.csv")
for index, row in tqdm(df.iterrows()):
    try:
        # title = clean_zh_text(row['title']) # 清理文本数据
        title = row['title']
        # text = clean_zh_text(row['text'])   # 清理文本数据
        text = row['text']   
    
        title_words = ltpFunc.ltp_seg(title)    # 对标题分词
        title_pos = ltpFunc.ltp_pos(title_words)    # 对标题进行词性标注
        title_per, title_loc, title_org = ltpFunc.ltp_ner(title_words, title_pos)   # 对标题进行实体抽取

        text_words = ltpFunc.ltp_seg(text)  # 对正文进行分词
        text_pos = ltpFunc.ltp_pos(text_words)  # 对正文进行词性标注
        text_per, text_loc, text_org = ltpFunc.ltp_ner(text_words, text_pos)    # 对正文进行词性标注

        tmp = {
            'title': title,
            'text': text,
            'title_words': title_words,
            'title_per': title_per,
            'title_loc': title_loc,
            'title_org': title_org,
            'text_words': text_words,
            'text_per': text_per,
            'text_loc': text_loc,
            'text_org': text_org
        }
    except:
        print(row)
    result_list.append(tmp)

# 将处理结果输出到文件查看效果
with codecs.open("result/ltp_result.txt", "w", "utf-8") as wf:
    for t in result_list:
        for key in t.keys():
            wf.write(key + ": " + str(t[key]) + "\n")
        wf.write("\n")
'''

'''
# 根据关键词检索数据
df = pd.read_csv("data/南海_2018-01-01_2020-02-10.csv")
df = df = df.dropna(subset=["content", "title"]) # 删除content, title中值为Nan的行
result = df[df['original_text'].str.contains('海军|南海|航行')] # https://blog.csdn.net/htbeker/article/details/79645651
result.to_csv("result/南海航行观点筛选.csv", index=False)
# print(df['news_id'])
'''

# 根据news_id检索数据库中的观点
theme_name = "南海自由航行"
news_df = pd.read_csv("data/" + theme_name + "_news.csv")
news_id = list(news_df.news_id) # 将数据中的news_id提取出来送入观点库中提取
vps = find_viewpoints_by_news_id(news_id)   # 从观点库中根据news_id查找对应的观点
vps_list = []
for v in vps:
    # print(v.__dict__['_d_'])
    vps_list.append(v.__dict__['_d_'])

vps_df = pd.DataFrame(vps_list)
vps_df.to_csv("data/" + theme_name + "_views.csv", index=False) # 将观点数据存入文件中

