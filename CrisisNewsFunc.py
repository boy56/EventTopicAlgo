# -*- coding: UTF-8 -*-
'''
危机新闻识别
1、识别危机新闻并计算新闻的危机指数(根据title与content判断)
'''
import codecs
import json
from typing import Iterator, List, Dict
import itertools
import random

class CrisisNewsFunc:
    def __init__(self):
        # self.wj_words_dict = self._load_dict("dict/WJWords.json")    # 加载WJ关键词
        # self.wj_words_dict_en = self._load_dict("dict/WJWords_en.json") # 英文版
        # self.wj_words_dict_ja = self._load_dict("dict/WJWords_ja.json") # 日文版
        # self.wj_words_dict_ko = self._load_dict("dict/WJWords_ko.json") # 韩文版

        self.wj_words_dict_pro = self._load_dict("dict/WJWordsPro.json") # 升级版危机指数计算算法


    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CrisisNewsFunc, cls).__new__(cls)
        return cls.instance

    # 返回值为dict类型
    def _load_dict(self, path:str) -> Dict:
        # 加载危机事件关键词字典
        with codecs.open(path,'r','utf-8') as rf:
            wj_words_dict = json.load(rf)
        return wj_words_dict

    '''
    # 通过新闻的title和content识别WJ新闻, 并计算新闻的危机指数
    def calcu_crisis(self, theme, title: str, content: str, language='zh') -> str:
        
        WJWords_set = set()

        # 首先当前专题下的WJ关键词
        if language == 'zh':
            WJWords_set = set(self.wj_words_dict[theme])
        elif language == 'en':
            WJWords_set = set(self.wj_words_dict_en[theme])
        elif language == 'ja':
            WJWords_set = set(self.wj_words_dict_ja[theme])
        elif language == 'ko':
            WJWords_set = set(self.wj_words_dict_ko[theme])

        WJWord_count = 0
        WJWords = {}
        crisis_flag = False # 标记其是否是WJ事件
        
        # WJ事件识别阶段
        # 首先根据title判断其是否为WJ事件
        for w in WJWords_set:
            if w in title:
                crisis_flag = True
                break
        
        # WJ事件指数计算
        # 然后根据内容计算其危机指数
        if crisis_flag:
            for w in WJWords_set:
                wj_num = content.count(w)
                if wj_num > 0:
                    WJWord_count += wj_num
                    WJWords[w] = wj_num
        
        # 如果WJWrod_count=0,即当前新闻不是危机事件
        # 返回初步的危机指数以及对应的危机关键词
        return WJWord_count, " ".join([key+':'+str(value) for key, value in WJWords.items()])
    '''
    
    # 危机指数计算改进版
    def calcu_crisis_pro(self, theme, title: str, language='zh'):
        theme_crisis_list = self.wj_words_dict_pro[theme]
        wordpair_w_dict = {} # 词对对title的风险度
        # print(title)
        max_crisis = 0 # 可能会出现多个词对匹配的情况, 选取出现的最大值
        WJWordPairs = {} # 记录匹配到的所有词对, 并给出其原始weight

        # 根据字典构造wordpair_w_dict
        for word_str_dict in theme_crisis_list:
            for word_str, weight in word_str_dict.items(): # 一般只有一个词条 
                data_list = []
                and_word_list = word_str.replace(" ","").split(',') # 先去除空格再划分
                for or_words in and_word_list:
                    data_list.append(or_words.split('/'))
                # 使用笛卡尔积来组合词条
                for item in itertools.product(*data_list):
                    wordpair_w_dict[item] = weight

        for wordpair in wordpair_w_dict.keys():
            # print(wordpair)
            flag = True
            weight = wordpair_w_dict[wordpair]
            for w in wordpair:
                if w not in title:
                    flag = False
                    break
            if flag: 
                crisis_w = weight + random.randint(-10, 20)
                if crisis_w > max_crisis:
                    max_crisis = crisis_w
                WJWordPairs["-".join(wordpair)] = crisis_w

        return max_crisis, " ".join([key+':'+str(value) for key, value in WJWordPairs.items()])

if __name__ == '__main__':
    
    crisisNewsFunc = CrisisNewsFunc()
    theme = "南海"
    title = "原创航行自由换人了?美海岸警卫队接替美海军巡航,目前已闯进黄海。"
    print(crisisNewsFunc.calcu_crisis_pro(theme, title))