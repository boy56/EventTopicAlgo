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




# 数据库字段更新
if __name__ == "__main__":
    theme = "南海"
    title = "原创航行自由换人了?美海岸警卫队接替美海军巡航,目前已闯进黄海。"
    crisisNewsFunc = CrisisNewsFunc()
    # NewsInfo.update({'crisis': crisisNewsFunc.calcu_crisis_pro(theme, NewsInfo.title)[0], 'wjwords': crisisNewsFunc.calcu_crisis_pro(theme, NewsInfo.title)[1]}).where(NewsInfo.theme_label==theme).execute()