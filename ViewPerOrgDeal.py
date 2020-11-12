# -*- coding: UTF-8 -*-
'''
机构职位字段补全函数(作用于数据库view表)
1、遍历数据库, 更新本地存储的专家表
2、遍历数据库, 根据本地存储的专家表补全缺失字段
'''
from models import NewsInfo, ViewsInfo, mysql_db, OtherNewsInfo
import codecs
from tqdm import tqdm
import json
from utils import clean_zh_text

class ViewPerOrgDeal:
    def __init__(self):
        
        # 加载中文国家名称转换信息
        with codecs.open("dict/per_org.json",'r','utf-8') as jf:
            self.per_org_dict = json.load(jf)


    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ViewPerOrgDeal, cls).__new__(cls)
        return cls.instance

    # 根据数据表, 更新本地字典
    def update_dict(self, all_update=True, per=""):  # 更新全部数据还是一个专家的
        if all_update:
            views = ViewsInfo.select()
        else:
            views = ViewsInfo.select().where(ViewsInfo.personname == per)
        
        for v in tqdm(views):
            per_name = clean_zh_text(v.personname, cleantype=3)
            if len(per_name) < 2: continue
            if per_name in self.per_org_dict:
                # 根据条件更新数据, 尽可能补全
                if len(v.orgname) >= 2 and len(self.per_org_dict[per_name]['orgname']) < 2:
                    self.per_org_dict[per_name]['orgname'] = v.orgname
                if len(v.pos) >= 2 and len(self.per_org_dict[per_name]['pos']) < 2:
                    self.per_org_dict[per_name]['pos'] = v.pos
            else:
                self.per_org_dict[per_name] = {
                    "orgname": v.orgname,
                    "pos": v.pos
                }
        # 将更新后的专家信息保存到json中        
        with codecs.open("dict/per_org.json", "w", "utf-8") as wf:
            json.dump(self.per_org_dict, wf, indent=4, ensure_ascii=False)


    # 更新数据库中的数据
    def update_sql_data(self, all_update=True, per=""):  # 更新全部数据还是一个人的
        if all_update:
            views = ViewsInfo.select()
        else:
            views = ViewsInfo.select().where(ViewsInfo.personname == per)
        
        for v in tqdm(views):
            per_name = clean_zh_text(v.personname, cleantype=3)
            if len(per_name) < 2: continue
            if per_name not in self.per_org_dict: continue
            if len(v.orgname) >= 2 and len(v.pos) >= 2: continue
            # 根据条件更新数据库中数据
            if len(v.orgname) < 2 and len(self.per_org_dict[per_name]['orgname']) >= 2:
                v.orgname = self.per_org_dict[per_name]['orgname']
            if len(v.pos) < 2 and len(self.per_org_dict[per_name]['pos']) >= 2:
                v.pos = self.per_org_dict[per_name]['pos']
            
            # 将修改提交到数据库            
            v.save()


if __name__ == '__main__':
    
    viewPerOrgDealFunc = ViewPerOrgDeal()

    # viewPerOrgDealFunc.update_dict(all_update=True)
    viewPerOrgDealFunc.update_sql_data(all_update=True)
