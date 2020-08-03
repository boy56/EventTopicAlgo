import pandas as pd
from utils import clean_zh_text
from datetime import datetime
from models import NewsInfo, ViewsInfo, mysql_db, OtherNewsInfo
import math
import os
import time
from tqdm import tqdm
import argparse
from peewee import chunked
from CrisisNewsFunc import CrisisNewsFunc
from EventPredictFunc import EventPredictFunc


# 数据库字段更新
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='data2sql')
    parser.add_argument('--mode', type=str, default='both', help="choose a mode: ['crisis', 'nextevent', 'both']")
    # parser.add_argument('--theme', default='NH', type=str, help='theme_name')
    args = parser.parse_args()


    # theme = "南海" # 更新的主题范围, 南海、朝核、台选, both
    
    mode = args.mode # 更新crisis、nextevent or both
    crisisNewsFunc = CrisisNewsFunc()
    eventPredictFunc = EventPredictFunc()

    # news = NewsInfo.select().where(NewsInfo.theme_label == theme) # 根据主题选取新闻
    news = NewsInfo.select()

    # 逐条更新 (是否有批量更新方法？)
    for n in tqdm(news):
        if mode == "crisis":
            WJcrisis, WJWords = crisisNewsFunc.calcu_crisis_pro(n.theme_label, n.title) # 危机指数计算
            n.crisis = WJcrisis
            n.wjwords = WJWords
            n.save()
        elif mode == "nextevent":
            n.nextevent = eventPredictFunc.calcu_next_event(n.theme_label, n.title) # 事件预测
            n.save
        elif mode == "both":
            WJcrisis, WJWords = crisisNewsFunc.calcu_crisis_pro(n.theme_label, n.title)
            n.crisis = WJcrisis
            n.wjwords = WJWords
            n.nextevent = eventPredictFunc.calcu_next_event(n.theme_label, n.title)
            n.save()
        else:
            print("Error: mode not in ['crisis', 'nextevent', 'both']")


