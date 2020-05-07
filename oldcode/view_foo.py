# 根据文本内容得到对应观点信息的API, 实验室内部本地访问接口
import json
import requests
url = 'http://10.1.1.56:8800/news_to_vp_api/'
news_list = ['中央军委主席习近平认为这次阅兵十分成功。']
str1 = json.dumps(news_list,ensure_ascii=False)
data = {'news_list':str1}

response = requests.post(url,data=data)
print(response.text)