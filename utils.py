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


import re
from fuzzywuzzy import fuzz


# 计算文本相似度(基于编辑距离)
def text_sim(s1, s2):
    return fuzz.ratio(s1, s2)

# keep English, digital and Chinese
def clean_zh_text(text):
    text = text.replace('，',',').replace('。','.').replace('？','?').replace('！','!').replace('：',':') # 将英文标点转换为中文标点
    comp = re.compile('[^A-Z^.^,^!^?^:^a-z^0-9^\u4e00-\u9fa5]')
    return comp.sub('', text)

