# 事件分析页面数据处理

import pandas as pd
import json
import codecs
from tqdm import tqdm
from datetime import datetime
import pickle
from fuzzywuzzy import fuzz
import time
from utils import clean_zh_text


news_df = pd.read_csv("data/news_demo.csv")
views_df = pd.read_csv("data/views_demo.csv")

# 遍历新闻数据, 获取相关信息
time_news_dict = {}
nextevent_dict = {} # 事件预测字典处理 {event: weight}
nextevent_news = {} # 事件预测触发新闻title {event: newslist}
nextevent_views = {} # 事件预测的支撑观点(从支撑新闻中选取) {event: newsid_list}

nextevent_news_pro = {}
nextevent_views_pro = {}
nextevent_timeline_data = {}
nextevent_graph_data = {} # 根据支撑素材构造的图谱数据

nextevent_media_dict = {} # [event: media_set]
nextevent_tri_dict = {} # [event: tri_set]
nextevent_org_dict = {}
nextevent_per_dict = {}
nextevent_view_dict = {}

title_set = set()
news_df['time'] = pd.to_datetime(news_df['time'])
for index, n in news_df.iterrows():
    # print(type(news.viewsinfo_set))
    sim_flag = False
    n_title = n.title.replace("原创",'').replace("转帖",'').replace("参考消息",'')
    if len(n_title) < 10: continue
    for old_t in title_set:
        if fuzz.partial_ratio(n_title, old_t) > 80:
            sim_flag = True
            break
    if sim_flag: continue

    #转换成新的时间格式(2016-05-05 20:28:54)
    time_str = n.time.strftime("%Y-%m-%d %H:%M:%S")
    
    # 事件预测数据处理
    event_list = n.nextevent.split(',') # 根据','分割多个候选事件
    for e in event_list:
        e_str, weight = e.split(':')
        if e_str in nextevent_dict:
            if e_str != '无风险事件':
                
                # print(n.newsid)
                # print(n.customer)
                nextevent_news[e_str].append(n_title + " " + time_str + " " + n.customer)
                nextevent_views[e_str].append(n.newsid)
                nextevent_timeline_data[e_str].append({
                    'id': n.newsid,
                    'title': n_title,
                    'content': n.content,
                    'url':"",
                    'foreign': False,
                    'dateDay': time_str
                })
                
                # 增加支撑新闻信息
                tmp = {}
                tmp['id'] = n.newsid
                tmp['title'] = n_title
                tmp['content'] = n.content
                tmp['time'] = time_str
                tmp['source'] = n.customer
                tmp['crisis'] = n.crisis
                # 增加NEW、MEDIA类型的节点
                nextevent_graph_data[e_str]['nodelist'].append(
                    {
                        "ID": n.newsid,
                        "name": n_title + " " + time_str,
                        "type": "NEW",
                        "category": 0,
                        "symbolSize": 10
                    }
                )
                if n.customer not in nextevent_media_dict[e_str]:
                    nextevent_graph_data[e_str]['nodelist'].append(
                        {
                            "ID": n.customer,
                            "name": n.customer,
                            "type": "MEDIA",
                            "category": 1,
                            "symbolSize": 10
                        }
                    )
                    nextevent_media_dict[e_str].add(n.customer)
                nextevent_graph_data[e_str]['linklist'].append(
                    {
                        "source": n_title + " " + time_str,
                        "target": n.customer,
                        "symbolSize": 10
                    }
                )
                # 处理新闻title, 根据新闻危机词高亮新闻title, 在原字符串增加html高亮标签
                wjword_set = set()
                if len(str(n.wjwords)) > 3:
                    for wjwords in n.wjwords.split(" "):
                        trigger = wjwords.split(":")[0]
                        # 增加 NEW 与 Tri 之间的关系
                        if trigger not in nextevent_tri_dict[e_str]:
                            nextevent_graph_data[e_str]['nodelist'].append(
                                {
                                    "ID": trigger,
                                    "name": trigger,
                                    "type": "TRIGGER",
                                    "category": 2,
                                    "symbolSize": 12
                                }
                            )
                            nextevent_tri_dict[e_str].add(trigger)
                            # 增加 Tri 与 NEXTEVENT之间的关系
                            nextevent_graph_data[e_str]['linklist'].append(
                                {
                                    "source": trigger,
                                    "target": e_str,
                                    "symbolSize": 10
                                }
                            )

                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": n_title + " " + time_str,
                                "target": trigger,
                                "symbolSize": 10
                            }
                        )
                        for w in wjwords.split(":")[0].split("-"):
                            if len(w) > 0: wjword_set.add(w)
                
                for w in wjword_set:
                    html_str = '<span style="color: red;">' + w + '</span>'
                    tmp['title'] = tmp['title'].replace(w, html_str)

                nextevent_news_pro[e_str].append(tmp)

                new_views = views_df[views_df['source'] == str(n.newsid)]
                new_views = new_views.dropna(subset=["per"])
                new_views.fillna(0)
                views_list = []
                # 根据事件子图去重

                for index, v in new_views.iterrows():
                    sim_flag = False
                    org_str = clean_zh_text(str(v.org) + str(v.pos), 3)
                    if v.viewpoint in nextevent_view_dict[e_str]: continue    # 观点去重
                    if len(v.viewpoint) < 10: continue 
                    if len(org_str + str(v.per)) < 2: continue
                    for old_v in nextevent_view_dict[e_str]:
                        if fuzz.partial_ratio(v.viewpoint, old_v) > 70:
                            sim_flag = True
                            break
                    if sim_flag: continue
                    views_list.append(
                        {
                            "org": org_str + str(v.per),
                            "viewpoint": v.verb + v.viewpoint,
                            "eventname": e_str,
                            "time": v.time,
                            "weight": 1,
                            "recommend": 0
                        }
                    )
                    if len(str(v.per)) > 1: # 人名字符串长度大于1才进行处理
                        # 增加观点节点
                        nextevent_graph_data[e_str]['nodelist'].append(
                            {
                                "ID": v.viewid,
                                # "name": v.verb + v.viewpoint + " " + v.time.strftime('%Y-%m-%d %H:%M:%S'),
                                "name": v.verb + v.viewpoint,
                                "type": "VIEW",
                                "category": 4,
                                "symbolSize": 10
                            }
                        )
                        # 增加新闻与人名之间的关系
                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": n_title + " " + time_str,
                                "target": v.per,
                                "symbolSize": 10
                            }
                        )
                        # 增加人名与观点间的关系
                        if v.per not in nextevent_per_dict[e_str]:
                            nextevent_graph_data[e_str]['nodelist'].append(
                                {
                                    "ID": v.per,
                                    "name": v.per,
                                    "type": "PERSON",
                                    "category": 3,
                                    "symbolSize": 10
                                }
                            )
                            nextevent_per_dict[e_str].add(v.per)
                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": v.verb + v.viewpoint,
                                "target": v.per,
                                "symbolSize": 10
                            }
                        )
                        
                        if len(org_str) > 2 and (org_str != "nannan"):
                            # 增加人名与职位间的关系
                            if org_str not in nextevent_org_dict[e_str]:
                                nextevent_graph_data[e_str]['nodelist'].append(
                                    {
                                        "ID": org_str,
                                        "name": org_str,
                                        "type": "ORG",
                                        "category": 5,
                                        "symbolSize": 10
                                    }
                                )
                                nextevent_org_dict[e_str].add(org_str)
                            nextevent_graph_data[e_str]['linklist'].append(
                                {
                                    "source": v.per,
                                    "target": org_str,
                                    "symbolSize": 10
                                }
                            )
                    nextevent_view_dict[e_str].add(v.viewpoint)
                nextevent_views_pro[e_str].extend(views_list)

        else:
            if e_str != '无风险事件':
                nextevent_dict[e_str] = 0.9
                nextevent_news[e_str] = [n_title + " " + time_str + " " + n.customer]
                nextevent_views[e_str] = [n.newsid]
                nextevent_timeline_data[e_str] = [{
                    'id': n.newsid,
                    'title': n_title,
                    'content': n.content,
                    'url': "",
                    'foreign': False,
                    'dateDay': time_str
                }]
                nextevent_graph_data[e_str] = {}
                nextevent_graph_data[e_str]['nodelist'] = []
                nextevent_graph_data[e_str]['linklist'] = []
                nextevent_graph_data[e_str]['categories'] = [
                    {'name':"情报"},
                    {'name':"媒体"},
                    {'name':"触发词"},
                    {'name':"人物"},
                    {'name':"观点"},
                    {'name':"职位"},
                    {'name':"待预测事件"}
                ]

                nextevent_media_dict[e_str] = set() # 针对事件子图的节点去重设计
                nextevent_tri_dict[e_str] = set()
                nextevent_view_dict[e_str] = set()

                # 增加待预测事件节点
                nextevent_graph_data[e_str]['nodelist'].append({
                    "ID": e_str,
                    "name": e_str,
                    "type": "NEXTEVENT",
                    "category": 6,
                    "symbolSize": 14
                })


                # 增加支撑新闻信息
                tmp = {}
                tmp['id'] = n.newsid
                tmp['title'] = n_title
                tmp['content'] = n.content
                tmp['time'] = time_str
                tmp['source'] = n.customer
                tmp['crisis'] = n.crisis

                # 增加NEW、MEDIA类型的节点
                nextevent_graph_data[e_str]['nodelist'].append(
                    {
                        "ID": n.newsid,
                        "name": n_title + " " + time_str,
                        "type": "NEW",
                        "category": 0,
                        "symbolSize": 10
                    }
                )
                if n.customer not in nextevent_media_dict[e_str]:
                    nextevent_graph_data[e_str]['nodelist'].append(
                        {
                            "ID": n.customer,
                            "name": n.customer,
                            "type": "MEDIA",
                            "category": 1,
                            "symbolSize": 10
                        }
                    )
                    nextevent_media_dict[e_str].add(n.customer)
                nextevent_graph_data[e_str]['linklist'].append(
                    {
                        "source": n_title + " " + time_str,
                        "target": n.customer,
                        "symbolSize": 10
                    }
                )

                # 处理新闻title, 根据新闻危机词高亮新闻title, 在原字符串增加html高亮标签
                wjword_set = set()
                if len(str(n.wjwords)) > 3:
                    for wjwords in n.wjwords.split(" "):
                        trigger = wjwords.split(":")[0]
                        # 增加 NEW 与 Tri 之间的关系
                        if trigger not in nextevent_tri_dict[e_str]: # 如果未出现过改节点则新加
                            nextevent_graph_data[e_str]['nodelist'].append(
                                {
                                    "ID": trigger,
                                    "name": trigger,
                                    "type": "TRIGGER",
                                    "category": 2,
                                    "symbolSize": 12
                                }
                            )
                            nextevent_tri_dict[e_str].add(trigger)
                            
                            nextevent_graph_data[e_str]['linklist'].append(
                                {
                                    "source": trigger,
                                    "target": e_str,
                                    "symbolSize": 10
                                }
                            )
                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": n_title + " " + time_str,
                                "target": trigger,
                                "symbolSize": 10
                            }
                        )
                        for w in wjwords.split(":")[0].split("-"):
                            wjword_set.add(w)
                
                for w in wjword_set:
                    html_str = '<span style="color: red;">' + w + '</span>'
                    tmp['title'] = tmp['title'].replace(w, html_str)

                nextevent_news_pro[e_str] = [tmp]

                # 处理观点数据
                new_views = views_df[views_df['source'] == str(n.newsid)]
                new_views = new_views.dropna(subset=["per"])
                new_views.fillna(0)
                views_list = []
                # 根据事件子图去重
                nextevent_per_dict[e_str] = set() # 用于节点去重
                nextevent_org_dict[e_str] = set()

                for index, v in new_views.iterrows():
                    sim_flag = False
                    org_str = clean_zh_text(str(v.org) + str(v.pos), 3)
                    if v.viewpoint in nextevent_view_dict[e_str]: continue    # 观点去重
                    if len(v.viewpoint) < 10: continue 
                    if len(org_str + str(v.per)) < 2: continue
                    for old_v in nextevent_view_dict[e_str]:
                        if fuzz.partial_ratio(v.viewpoint, old_v) > 70:
                            sim_flag = True
                            break
                    if sim_flag: continue
                    views_list.append(
                        {
                            "org": org_str + str(v.per),
                            "viewpoint": v.verb + v.viewpoint,
                            "eventname": e_str,
                            "time": v.time,
                            "weight": 1,
                            "recommend": 0
                        }
                    )
                    if len(str(v.per)) > 1: # 人名字符串长度大于1才进行处理
                        # 增加观点节点
                        nextevent_graph_data[e_str]['nodelist'].append(
                            {
                                "ID": v.viewid,
                                # "name": v.verb + v.viewpoint + " " + v.time.strftime('%Y-%m-%d %H:%M:%S'),
                                "name": v.verb + v.viewpoint,
                                "type": "VIEW",
                                "category": 4,
                                "symbolSize": 10
                            }
                        )
                        # 增加新闻与人名之间的关系
                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": n_title + " " + time_str,
                                "target": v.per,
                                "symbolSize": 10
                            }
                        )
                        # 增加人名与观点间的关系
                        if v.per not in nextevent_per_dict[e_str]:
                            nextevent_graph_data[e_str]['nodelist'].append(
                                {
                                    "ID": v.per,
                                    "name": v.per,
                                    "type": "PERSON",
                                    "category": 3,
                                    "symbolSize": 10
                                }
                            )
                            nextevent_per_dict[e_str].add(v.per)
                        nextevent_graph_data[e_str]['linklist'].append(
                            {
                                "source": v.verb + v.viewpoint,
                                "target": v.per,
                                "symbolSize": 10
                            }
                        )
                        
                        if len(org_str) > 2 and (org_str != "nannan"):
                            # 增加人名与职位间的关系
                            if org_str not in nextevent_org_dict[e_str]:
                                nextevent_graph_data[e_str]['nodelist'].append(
                                    {
                                        "ID": org_str,
                                        "name": org_str,
                                        "type": "ORG",
                                        "category": 5,
                                        "symbolSize": 10
                                    }
                                )
                                nextevent_org_dict[e_str].add(org_str)
                            nextevent_graph_data[e_str]['linklist'].append(
                                {
                                    "source": v.per,
                                    "target": org_str,
                                    "symbolSize": 10
                                }
                            )



                    nextevent_view_dict[e_str].add(v.viewpoint)
                nextevent_views_pro[e_str] = views_list

    title_set.add(n_title)

result = {}
result['nextevent_news_pro'] = nextevent_news_pro # 用于事件预测的支撑材料
result['nextevent_views_pro'] = nextevent_views_pro # 用于事件预测的支撑观点
result['graph_data'] = nextevent_graph_data # 支撑材料转化的图谱数据
result['nextevent_timeline_data'] = nextevent_timeline_data # 将用于支撑事件预测的新闻按照时间轴排列
result['nextevent_weight'] = nextevent_dict

with codecs.open("data/event_demo.json", "w", "utf-8") as wf:
    json.dump(result, wf, indent=4, ensure_ascii=False)


