1、项目说明: 主要用于武汉项目的数据获取、离线数据处理以及数据入库
2、运行顺序: 
    (1) search_news.py -> 从ES中获取关键词典中的新闻数据
    (2) news2views.py -> 对新闻数据进行预处理, 并从观点系统获取观点数据, 分别生成_newslabel.csv(处理后新闻数据)、_views.csv(观点数据)文件
    (3) datatosql.py -> 将新闻数据、观点数据导入mysql中