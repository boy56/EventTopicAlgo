1、项目说明: 主要用于武汉项目的数据获取、离线数据处理以及数据入库
2、运行顺序: 
    (1) search_news.py -> 从ES中获取关键词典中的新闻数据
    (2) datapre.py -> 对新闻数据进行预处理, 增加新的标签, 生成_news_newdata.csv, _views_newdata.csv, 并更新{专家:国籍}字典
    (3) data2sql.py -> 将新闻数据(_news_newdata.csv)、观点数据(_views_newdata.csv)导入mysql中
    (4) data_update_sql.py -> 根据特定需要利用改进的算法更新数据库中的数据字段, 主要更新crisis、wjwords(危机事件识别)以及nextevent(事件预测)
3、拷贝文件:
    将dict文件夹中的 'themename'_countryviews_dict.pkl拷入EventTopicBackend项目dict中, 用于主页面的地图显示