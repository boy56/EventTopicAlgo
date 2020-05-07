# 进行函数的测试用
from ClassifyFunc import ClassifyFunc

def title_classify_test():
    classifyFunc = ClassifyFunc(theme="南海")
    
    # title_words = ['菲', '总统', '杜特', '尔特', '警告', '美国', '：', '别', '在', '南', '海军', '演', '挑衅', '中国']
    title = "菲总统杜特尔特警告美国：别在南海军演挑衅中国"
    print(classifyFunc.classify_title(title, 1))

if __name__ == '__main__':
    title_classify_test()