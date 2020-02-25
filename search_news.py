import happybase
import datetime
import  csv
import pandas as pd                         #导入pandas包
import os
HBASE_IP = '10.1.1.16'
HBASE_PORT = 9090
HBASE_TABLE = 'terren'
conn= happybase.Connection(HBASE_IP, HBASE_PORT)
table = conn.table(HBASE_TABLE)

from elasticsearch import Elasticsearch
es = Elasticsearch(['http://10.1.1.35:9200'])

def get_news_row_key(doc_id, time_str):
    assert type(time_str) == str

    if len(time_str.strip()) > 19:
        time = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        time = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
    time_str = (time + datetime.timedelta(hours=8)).strftime("%Y%m%d%H%M%S")
    return time_str + "-" + doc_id
def get_hbase_row(doc_id, time_str):
    #r = map(get_news_row_key,doc_id_list, time_str_list)
    r=get_news_row_key(doc_id=doc_id,time_str=time_str)
    print(r)
    result = table.row(r)
    info={}
    for key in result:
        key_s=str(key,encoding='utf-8').replace("info:","")
        value_s=str(result[key],encoding='utf-8')
        info[key_s]=value_s
    return info
def get_news_data(fout):
    name=['news_id','app', 'area', 'comments', 'content', 'country', 'customer', 'emotion', 'entities', 'keyword', 'location', 'pageview', 'phonecompany', 'publishDay', 'referdomain', 'searchWord', 'searchengine', 'sourceType', 'time', 'title', 'url', 'userview', 'words']
    csvFile = open(fout, "w",newline='',encoding='utf-8')  # 创建csv文件
    writer = csv.writer(csvFile)  # 创建写的对象
    body_={"query":{"bool":{"must":[{"query_string":{"default_field":"_all","query":"南海"}},{"query_string":{"default_field":"_all","query":"航行"}},{"query_string":{"default_field":"_all","query":"自由"}}],"must_not":[],"should":[]}},"from":0,"size":5000,"sort":[],"aggs":{}}
    res=es.search(index="terren_v2",body=body_)
    data=res['hits']['hits']
    print(len(data))
    writer.writerow(name)  # 写入列的名称
    count=0
    for i in range(len(data)):
        try:
            result=get_hbase_row(doc_id=data[i]['_id'],time_str=data[i]['_source']['time'])
        except:
            continue
        count=count+1
        if result == {}:
            continue;
        row = []
        row.append(data[i]['_id'])
        for item in name[1:]:
            if item in result:
                row.append(result[item])
            else:
                row.append("")
        writer.writerow(row)
    csvFile.close()
    print(count)
get_news_data('新闻列表.csv')