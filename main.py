# 进行函数的测试用
from utils import classify_title

def title_classify_test():
    dict_path = "dict/内容分类.txt"
    title_words = ['菲', '总统', '杜特', '尔特', '警告', '美国', '：', '别', '在', '南', '海军', '演', '挑衅', '中国']
    print(classify_title(title_words, dict_path, 2))

if __name__ == '__main__':
    title_classify_test()