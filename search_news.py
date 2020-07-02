import happybase
import datetime
import  csv
import pandas as pd                         #导入pandas包
import os
import codecs

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
    # print(r)
    result = table.row(r)
    info={}
    for key in result:
        key_s=str(key,encoding='utf-8').replace("info:","")
        value_s=str(result[key],encoding='utf-8')
        info[key_s]=value_s
    return info

# 根据关键词构造查询体并以半年为时间段进行检索, 并将结果写入csv文件
def batch_search(query_list, time_start, time_end, outcsv):

    # 将时间限制放入查询体
    query_list.append({"range":{"time":{"gt":time_start,"lt":time_end}}})
    print(query_list)

    # 构建查询体
    body_ = {
        "query":
            {
                "bool":
                {
                    "must": query_list,
                    "must_not":[],
                    "should":[]
                }
            },
            "from":0,
            "size":5000,
            "sort":[],
            "aggs":{}
    }

    res = es.search(index="terren_v2",body=body_)
    data = res['hits']['hits']
    print(len(data))

    # 将数据写入fout
    count=0
    for i in range(len(data)):
        try:
            result=get_hbase_row(doc_id=data[i]['_id'],time_str=data[i]['_source']['time'])
        except:
            continue
        count=count+1
        if result == {}:
            continue
        row = []
        row.append(data[i]['_id'])
        for item in name[1:]:
            if item in result:
                row.append(result[item])
            else:
                row.append("")
        writer.writerow(row)

    query_list = query_list.pop() # 移除请求列表最后一个条件, 即当前查询的时间限制, 以防时间叠加问题

# fout为输出文件名称, fkeywords为需要爬取的关键词字典名称
def get_news_data(fout, fkeywords):
    name=['news_id','app', 'area', 'comments', 'content', 'country', 'customer', 'emotion', 'entities', 'keyword', 'location', 'pageview', 'phonecompany', 'publishDay', 'referdomain', 'searchWord', 'searchengine', 'sourceType', 'time', 'title', 'url', 'userview', 'words']
    csvFile = open(fout, "w",newline='',encoding='utf-8')  # 创建csv文件
    writer = csv.writer(csvFile)  # 创建写的对象
    writer.writerow(name)  # 写入列的名称


    # 读取待查询的关键词
    query_list = []
    with codecs.open(fkeywords,"r","UTF-8") as rf:
        for line in rf.readlines():
            word = line.strip()
            query_list.append(
                {"term":{"content": word}}  # 以该方式获得的关键词为'与'关系
            )
    
    # 以半年期进行分批次查询数据
    batch_search(query_list, '2018-01-01', '2018-06-01', writer)
    batch_search(query_list, '2018-06-02', '2018-12-31', writer)
    batch_search(query_list, '2019-01-01', '2019-06-01', writer)
    batch_search(query_list, '2019-06-02', '2019-12-31', writer)
    batch_search(query_list, '2020-01-01', '2020-07-01', writer) # 后续扩增数据可以注释掉这些代码, 然后在后面重新查询

    csvFile.close()

# 主函数
if __name__ == "__main__":    
    get_news_data('data/南海_202007_news.csv', 'dict/南海新闻关键词.txt')