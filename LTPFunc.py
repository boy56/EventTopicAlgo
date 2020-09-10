# -*- coding: UTF-8 -*-
# pyltp版本: 0.1.9.1

from pyltp import Segmentor, Postagger, NamedEntityRecognizer, Parser


# 分词, 输入为句子(str), 输出为分词列表
class LTPFunction:
    def __init__(self):
        self.segmentor = Segmentor()
        self.segmentor.load("model/cws.model")
        # self.segmentor.load_with_lexicon("model/cws.model", 'dict/segdict.txt') # 加载模型，第二个参数是您的外部词典文件路径
        self.postagger = Postagger()  # 初始化实例
        self.postagger.load('model/pos.model')  # 加载模型
        self.parser = Parser()  # 初始化实例
        self.parser.load('model/parser.model')  # 加载模型
        self.recognizer = NamedEntityRecognizer()  # 初始化实例
        self.recognizer.load('model/ner.model')

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(LTPFunction, cls).__new__(cls)
        return cls.instance

    def ltp_seg(self, sentence):
        words = self.segmentor.segment(sentence)
        return [i for i in words]

    # 词性标注, 输入为分词后的列表, 输出为词性标注列表
    def ltp_pos(self, word_list):
        # print(type(word_list))
        words_postags = self.postagger.postag(word_list)  # 词性标注
        # postagger.release()
        return [i for i in words_postags]

    # 实体抽取, 输入为分词列表、词性标注列表, 输出为人名集合、地名集合、机构名集合
    def ltp_ner(self, word_list, words_postags):
        netags = self.recognizer.recognize(word_list, words_postags)
        # print(" ".join(netags))
        entity = ''
        tag = ''
        person_set = set()
        location_set = set()
        organization_set = set()
        for i in range(len(netags)):
            ner = netags[i].split('-')
            if ner[0] == 'O':
                if entity != '':
                    if tag == 'Nh':
                        person_list.add(entity)
                    if tag == 'Ns':
                        location_list.add(entity)
                    if tag == 'Ni':
                        organization_list.add(entity)
                entity = ''
                tag = ''
            elif ner[0] == 'S':
                if ner[1] == 'Nh':
                    person_list.add(word_list[i])
                if ner[1] == 'Ns':
                    location_list.add(word_list[i])
                if ner[1] == 'Ni':
                    organization_list.add(word_list[i])
            elif ner[0] == 'B':
                entity = entity + word_list[i]
                tag = ner[1]
            elif ner[0] == 'I':
                entity = entity + word_list[i]
                tag = ner[1]
            else:
                entity = entity + word_list[i]
                tag = ner[1]
                if tag == 'Nh':
                    person_list.add(entity)
                if tag == 'Ns':
                    location_list.add(entity)
                if tag == 'Ni':
                    organization_list.add(entity)
                entity = ''
                tag = ''
        return person_set, location_set, organization_set

    # 句法分析, 输入为分词列表、词性标注列表, 输出为关系(Relation)列表、父节点(Head)列表
    def ltp_parser(self, word_list, pos_list):
        relation_list = []
        head_list = []
        arcs = self.parser.parse(word_list, pos_list)  # 句法分析
        for arc in arcs:
            relation_list.append(arc.relation)
            head_list.append(arc.head)

        return relation_list, head_list

    # 外部
    def ner_extract(self, title, content):

        # 最终得到的去重后的结果
        person_set = set()
        location_set = set()
        organization_set = set()

        # 根据title获取对应的信息
        title_words = self.ltp_seg(title)
        title_pos = self.ltp_pos(title_words)
        p_set, l_set, o_set = ltpFunction.ltp_ner(title_words, title_pos)

        # 将获取的集合放入结果中
        person_set = person_set | p_set
        location_set = location_set | l_set
        organization_set = organization_set | o_set
        
        # 根据content获取对应的信息 (是否分句后再处理)
        

        return list(person_set), list(location_set), list(organization_set)

# 主函数
if __name__ == "__main__":
    
    ltpFunction = LTPFunction()
    title_words = ltpFunction.ltp_seg("原创航行自由换人了?美海岸警卫队接替美海军巡航,目前已闯进黄海。")
    title_pos = ltpFunction.ltp_pos(title_words)

    person_list, location_list, organization_list = ltpFunction.ltp_ner(title_words, title_pos)

    print(person_list)
    print(location_list)
    print(organization_list)