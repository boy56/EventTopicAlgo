from elasticsearch_dsl import Document,Q
from elasticsearch import Elasticsearch
import math

ES_IP = '10.1.1.36'
ES_PORT = 9200
ES_IP_PORT = '%s:%d' % (ES_IP, ES_PORT)

class Doc(Document):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        return

    def set_from_dict(self, attr_dict):
        for key, value in attr_dict.items():
            setattr(self, key, value)
        return self


class ViewPoint(Doc):
    class Meta:
        doc_type = "msg"
        index = "viewpoint2"


def query_vp_by_news(news_ids):
        if not news_ids:
            return Q()
        return Q("terms", news_id=news_ids)

# 返回list<dict>
def find_viewpoints_by_news_id(news_ids=None,size=3000):
    es_client = Elasticsearch(ES_IP_PORT, timeout=600)
    vps_list = []
    slice_size = 500
    nslices = math.floor(len(news_ids) / slice_size)
    
    # 分批次查询
    for i in range(0, nslices):
        tmp_id = news_ids[i*slice_size:(i+1)*slice_size]    # 切片查询   
        q = query_vp_by_news(tmp_id)
        vps = ViewPoint.search().using(es_client).query(q).extra(size=size).execute()   # 输出为elasticsearch_dsl.response.Response对象
        for v in vps:
            # print(v.__dict__['_d_'])
            vps_list.append(v.__dict__['_d_'])
        # print(i," ", len(vps_list))
    # 将[nslices*slice_size:]进行处理
    tmp_id = news_ids[nslices*slice_size:]    # 切片查询   
    q = query_vp_by_news(tmp_id)
    vps = ViewPoint.search().using(es_client).query(q).extra(size=size).execute()   # 输出为elasticsearch_dsl.response.Response对象
    for v in vps:
        # print(v.__dict__['_d_'])
        vps_list.append(v.__dict__['_d_'])

    return vps_list
