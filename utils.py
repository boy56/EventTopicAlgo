# -*- coding: UTF-8 -*-

#                        .::::.
#                      .::::::::.
#                     :::::::::::
#                  ..:::::::::::'
#               '::::::::::::'
#                 .::::::::::
#            '::::::::::::::..
#                 ..:::::::::::::.
#               ``:::::::::::::::::
#                ::::``::::::::::'       .:::.
#               ::::'   ':::::'       .::::::::.
#             .::::'      ::::     .:::::::'::::.
#            .:::'       :::::  .:::::::::' ':::::.
#           .::'        :::::.:::::::::'      ':::::.
#          .::'         ::::::::::::::'         ``::::.
#      ...:::           ::::::::::::'              ``::.
#     ```` ':.          ':::::::::'                  ::::..
#                        '.:::::'                    ':'````..
#                     Beauty bless never bug

from typing import Iterator, List, Dict
import re
from fuzzywuzzy import fuzz
import codecs

# 依据新闻的评论计算新闻的影响力指数
# news_df 为pandas中df类型数据
# 返回值result为字典类型数据
def news_comment_deal(news_df):
    comment_news = news_df.dropna(subset=["content", "title", "comments"]) # 删除content, title, comments中值为Nan的行
    result = {}
    influence_list = []
    for index, row in comment_news.iterrows():
        news_id = row['news_id']
        influence_rate = 0
        comment_list = []
        # 依据每条评论的agreeCount与against 计算新闻的影响力  
        for comment in eval(row['comments']):  # 利用eval函数将字符串转换成list形式
            # comment = json.loads(comment)
            total_count = comment['against'] + comment['agreeCount']
            influence_rate += total_count + 10 # 评论支持数 + 评论反对数 + 10(评论权重)
            influence_list.append(influence_rate) # 用于后续对影响力参数进行调整
            comment['totalCount'] = total_count
            comment_list.append(comment)
            
                
        comment_list = sorted(comment_list,key = lambda e:e.__getitem__('totalCount'), reverse=True) # 依据totalCount对评论进行从大到小排序
        
        result[news_id] = {
            'influence_rate': influence_rate,
            'comment_list': comment_list
        }
    # 更新result中的影响力指数数据, 将数值映射到0-100
    max_influence = max(influence_list)

    for key in result.keys():
        result[key]['influence_rate'] = result[key]['influence_rate'] / max_influence * 100

    return result


# 计算文本相似度(基于编辑距离)
def text_sim(s1, s2):
    return fuzz.ratio(s1, s2)


# keep English, digital and Chinese
def clean_zh_text(text):
    text = text.replace('，',',').replace('。','.').replace('？','?').replace('！','!').replace('：',':') # 将英文标点转换为中文标点
    comp = re.compile('[^A-Z^.^,^!^?^:^a-z^0-9^\u4e00-\u9fa5]')
    return comp.sub('', text)


# 加载字典, type=1为国家字典, type=2为事件字典, 默认为国家字典
# 返回值为dict类型
def load_dict(path, type=1) -> Dict:
    result_dict = {}
    word_country_dict = {}
    with codecs.open(path,"r","UTF-8") as rf:
        for line in rf.readlines():
            category, keywords = line.strip().split(":") # 类别: 关键词1, 关键词2, ....
            keywords = set(keywords.split(","))
            result_dict[category] = keywords
    return result_dict



# 根据新闻标题进行分类, type=1为国家标签的分类, type=2为事件标签的分类
# 返回值为标签类型
def classify_title(title_words: List[str], path: str, type=1) -> str:
    # 首先加载关键词字典
    category_dict = load_dict(path, type)
    result_set = set()

    word_category_dict = {} # {word:category}, 此时一个单词对应一个事件类别, 未考虑多个一个单词对应多个事件类别的情况
    for category, keywords in category_dict.items():
        for w in keywords:
            word_category_dict[w] = category

    for w in title_words:
        if w in word_category_dict:
            result_set.add(word_category_dict[w])

    if len(result_set) == 0:
        result_set.add('其它')
    
    return ' '.join(result_set)

