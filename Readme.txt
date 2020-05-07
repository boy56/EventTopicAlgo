1、项目说明: 主要用于武汉项目的数据获取、离线数据处理以及数据入库
2、运行顺序: 
    (1) search_news.py -> 从ES中获取关键词典中的新闻数据
    (2) newsdeal.py -> 对新闻数据进行预处理, 生成_news_newlabel.csv(处理后新闻数据)
    (3) viewsdeal.py -> 根据_news_newlabel.csv文件从实验系统中获取观点数据, 生成_views.csv(处理后的观点数据)
    (4) datatosql.py -> 将新闻数据、观点数据导入mysql中, 并更新{专家: 国籍}字典