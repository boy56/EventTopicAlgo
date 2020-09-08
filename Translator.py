import datetime
import hashlib
import json
import random
import time
import pandas as pd
import requests


def translate(word):
    url = "http://api.fanyi.baidu.com/api/trans/vip/translate"
    appid = '20200908000561447'  # 你的appid
    secretKey = 'W4CisU0BYJ9pLjCNDA37'  # 你的密钥
    wordTrans = ""
    while len(word) > 4900:
        salt = random.randint(32768, 65536)  # 生成一个随机数
        sign = appid + word[0:4900] + str(salt) + secretKey  # 将appid和要翻译的字符、随机数、密钥组合成一个原始签名
        m = hashlib.new("md5")
        m.update(sign.encode(encoding="utf-8"))  # 注意使用utf-8编码
        msign = m.hexdigest()  # 得到原始签名的MD5值
        data = {
            "q": word[0:4900],
            "from": "auto",
            "to": "zh",
            "appid": appid,
            "salt": salt,
            "sign": msign
        }  # key 这个字典为发送给有道词典服务器的内容
        total = 0
        while total < 5:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                try:
                    result = json.loads(response.text)
                    # print(word[0:4900])
                    # print(result)
                    wordTrans += result['trans_result'][0]['dst']
                    break
                except Exception as e:
                    time.sleep(1)
                    print(total)
                    # print(response.text)
            else:
                print(response.status_code)
            total += 1
        time.sleep(1)
        word = word[4900:]
    salt = random.randint(32768, 65536)  # 生成一个随机数
    sign = appid + word[0:4900] + str(salt) + secretKey  # 将appid和要翻译的字符、随机数、密钥组合成一个原始签名
    m = hashlib.new("md5")
    m.update(sign.encode(encoding="utf-8"))  # 注意使用utf-8编码
    msign = m.hexdigest()  # 得到原始签名的MD5值
    data = {
        "q": word[0:4900],
        "from": "auto",
        "to": "zh",
        "appid": appid,
        "salt": salt,
        "sign": msign
    }  # key 这个字典为发送给有道词典服务器的内容
    total = 0
    while total < 5:
        response = requests.post(url, data=data)
        if response.status_code == 200:
            try:
                result = json.loads(response.text)
                # print(word[0:4900])
                # print(result)
                wordTrans += result['trans_result'][0]['dst']
                break
            except Exception as e:
                time.sleep(1)
                print(e)
                # print(total)
                # print(response.text)
        else:
            print(response.status_code)
        time.sleep(1)
        total += 1
    if len(wordTrans) > 0:
        return wordTrans
    else:
        return word

# 主函数
if __name__ == "__main__":

    df = pd.read_csv("data/other_language_data.csv")
    df['time'] = pd.to_datetime(df['time'])
    df = df.fillna('')  # 填充NA数据

    # 遍历读取处理    
    for index, row in df.iterrows():
        print(translate(row['title']))
        time.sleep(1)
        print(translate(row['content'])) 
        time.sleep(1)
