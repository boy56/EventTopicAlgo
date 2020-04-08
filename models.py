from peewee import *

mysql_db = MySQLDatabase(
    database = 'wuhandata',# string
    passwd = '123456', # string
    user = 'root', # string
    host = '127.0.0.1', # string
    port = 3306, # int
)

class NewsInfo(Model):
    
    # 自增的id
    # id = PrimaryKeyField()
    # id = IntegerField(primary_key=True, sequence=True)

    # redis原有字段
    newsid = CharField(primary_key=True, max_length = 64)  # 主键
    # area = CharField()    # 访客地区分布
    # comments = CharField()    # 用户评论
    # country = CharField() # 访客国家分布
    title = CharField()
    time = DateTimeField()
    content = TextField(default="")#无数据时默认为空字符串
    url = CharField()
    customer = CharField()  # 新闻的媒体分布
    emotion = IntegerField()    # 新闻情绪
    entities = CharField()  # 涉及实体
    keyword = CharField() # 关键词
    location = CharField() # 地点
    pageview = IntegerField() # 页面点击量
    userview = IntegerField() # 用户访问量
    # searchword = CharField()
    words = CharField()

    # 处理得到字段
    theme_label = CharField()
    content_label = CharField()
    country_label = CharField()


    class Meta:
        database = mysql_db
        # db_table = "newsinfo"

