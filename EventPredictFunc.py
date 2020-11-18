# -*- coding: UTF-8 -*-
'''
事件预测
1、对每个新闻计算其对候选事件的贡献值
'''
import codecs
import json
from typing import Iterator, List, Dict
import itertools

class EventPredictFunc:
    def __init__(self):
        self.ep_words_dict = self._load_dict("dict/EventPre.json")    # 加载WJ关键词
        
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(EventPredictFunc, cls).__new__(cls)
        return cls.instance

    # 返回值为dict类型
    def _load_dict(self, path:str) -> Dict:
        # 加载危机事件关键词字典
        with codecs.open(path,'r','utf-8') as rf:
            ep_words_dict = json.load(rf)
        return ep_words_dict

    # 通过新闻title计算后续可能发生的事件概率
    def calcu_next_event(self, theme, title):
        theme_ep_dict = self.ep_words_dict[theme]
        # wordpair_ep_dict= {} # 词对触发的候选事件
        # wordpair_w_dict = {} # 词对对候选事件的贡献度
        wordpair_event_weight_dict = {} # {(wordpair):{nextevent: weight}}

        # 根据字典构造wordpair_ep_dict 和 wordpair_w_dict
        for next_event, condition_list in theme_ep_dict.items():
            for word_str_dict in condition_list:
                for word_str, weight in word_str_dict.items(): # 一般只有一个词条
                    data_list = []
                    and_word_list = word_str.replace(" ","").split(',') # 先去除空格再划分
                    for or_words in and_word_list:
                        data_list.append(or_words.split('/'))
                    # 使用笛卡尔积来组合词条
                    for item in itertools.product(*data_list):
                        # wordpair_ep_dict[item] = next_event
                        # wordpair_w_dict[item] = weight
                        if item not in wordpair_event_weight_dict:
                            wordpair_event_weight_dict[item] = {}
                        wordpair_event_weight_dict[item][next_event] = weight

        # print(wordpair_ep_dict)
        # print(wordpair_w_dict)
        # print(wordpair_event_weight_dict)

        result = {} # 对候选事件的计算结果
        # 根据theme_ep_dict构造{词对:候选者事件}, 用每个词对匹配title获取其对候选事件的触发值
        # for wordpair in wordpair_ep_dict.keys():
        #     # print(wordpair)
        #     flag = True
        #     next_event = wordpair_ep_dict[wordpair]
        #     weight = wordpair_w_dict[wordpair]
        #     for w in wordpair:
        #         if w not in title:
        #             flag = False
        #             break
        #     if flag: 
        #         result[next_event] = weight
        #         break # 一个title默认只匹配一个词条
        
        # 每个title可以出发多个词对，每个词对可以导致多个后续事件
        for wp, nextevents in wordpair_event_weight_dict.items():
            flag = True # 判断是否符合词对条件
            for w in wp:
                if w not in title:
                    flag = False
                    break
            if flag:
                for next_event, weight in nextevents.items(): 
                    result[next_event] = weight

        # 如果没有候选事件则补无风险事件
        if not result:
            result['无风险事件'] = 1
            
        return ",".join([key+':'+str(value) for key, value in result.items()])

if __name__ == '__main__':
    
    eventPredictFunc = EventPredictFunc()
    theme = "南海"
    title = "美方在南海开展“航行自由行动”。"
    print(eventPredictFunc.calcu_next_event(theme, title))