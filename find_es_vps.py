from elasticsearch_dsl import Document,Q
from elasticsearch import Elasticsearch

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

def find_viewpoints_by_news_id(news_ids=None,size=3000):
    es_client = Elasticsearch(ES_IP_PORT, timeout=600)
    q = query_vp_by_news(news_ids[:1000])
    vps = ViewPoint.search().using(es_client).query(q).extra(size=size).execute()
    return vps
