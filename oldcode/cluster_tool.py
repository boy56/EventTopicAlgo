from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn import metrics
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer,TfidfVectorizer
from sklearn import preprocessing
import numpy as np
from gensim.models.word2vec import Word2Vec
from gensim import corpora
from gensim.models import lsimodel, ldamodel, tfidfmodel
#import Levenshtein

#from constants import Word2vecPath

# word2vec_path = "/home/LAB/yanhao/data/word2vec/zh_wiki_1G/wiki_chs_vec"
# Word2vecPath=""
# word2vec = Word2Vec.load(Word2vecPath)


def tfidf_bow(corpus_bow):
    tfidf = tfidfmodel.TfidfModel(corpus_bow)
    corpus_tfidf = tfidf[corpus_bow]
    return corpus_tfidf


def select_best_k(models, x):
    best_k = list(models.keys())[0]
    max_silhouette = -10000
    for k in list(models.keys()):
        labels = models[k].labels_
        try:
            silhouette = metrics.silhouette_score(x, labels, metric='euclidean')
            print(k, silhouette)
            if silhouette > max_silhouette:
                best_k = k
                max_silhouette = silhouette
        except Exception as e:
            print(e)
    if best_k <= 2:
        if best_k + 1 in models:                       # 至少两个类
            best_k += 1
    return best_k


def lda(corpus, num_topics, tfidf=False):
    dictionary = corpora.Dictionary(corpus)
    corpus = [dictionary.doc2bow(text) for text in corpus]
    if tfidf:
        corpus = tfidf_bow(corpus)
    lda_model = ldamodel.LdaModel(corpus=corpus, id2word=dictionary, num_topics=num_topics)
    result = lda_model.inference(corpus)
    topics = {topic: [] for topic in range(num_topics)}
    for i, r in enumerate(result[0]):
        max_topic = 0
        for topic_no in range(num_topics):
            if r[topic_no] > r[max_topic]:
                max_topic = topic_no
        topics[max_topic].append(i)
    return topics


def lsi(corpus, num_topics, tfidf=False):
    dictionary = corpora.Dictionary(corpus)
    corpus = [dictionary.doc2bow(text) for text in corpus]
    if tfidf:
        corpus = tfidf_bow(corpus)
    lsi_model = lsimodel.LsiModel(corpus=corpus, id2word=dictionary, num_topics=num_topics)
    result = lsi_model[corpus]
    topics = {topic: [] for topic in range(num_topics)}
    for i in range(len(corpus)):
        t = dict(result[i])
        if len(t) > 0:
            max_topic = list(t.keys())[0]
            for topic_no, value in t.items():
                if t[max_topic] < value:
                    max_topic = topic_no
            topics[max_topic].append(i)
    return topics



def k_means_tfidf(corpus, min_num, max_num):
    #docs = [" ".join(doc) for doc in corpus]
    docs=corpus
    print(docs)
    vectorizer = CountVectorizer()
    #vectorizer = TfidfVectorizer(min_df=1)
    transformer = TfidfTransformer()
    #vectorizer.fit_transform(docs)
    tfidf = transformer.fit_transform(vectorizer.fit_transform(docs))
    weight = tfidf.toarray()

    if len(corpus) > 2:
        models = {}
        num_topics_min = min(min_num, len(corpus) - 1)
        num_topics_max = min(max_num, len(corpus))
        # num_topics_max = min(len(corpus)// 10, len(corpus))
        step = max(2, (num_topics_max - num_topics_min) // 8)
        for k in range(num_topics_min, num_topics_max, step):
            # models[k] = KMeans(n_clusters=k).fit(weight)
            models[k] = AgglomerativeClustering(n_clusters=k).fit(weight)

        # select best k
        if len(corpus) > 2:
            bst_k = select_best_k(models, weight)
        else:
            bst_k = list(models.keys())[0]

        topics = {topic: [] for topic in range(bst_k)}
        for i, topic_no in enumerate(models[bst_k].labels_):
            topics[topic_no].append(i)

        return topics
    else:
        return {0: [i for i in range(len(corpus))]}


def cluster_by_string_similarity(corpus, threshold=0.3):

    result = {}
    #我找不到这个包，黄
    '''
    for i, doc in enumerate(corpus):
        for k in result:
            if Levenshtein.ratio(doc, k) > threshold:
                result[k].append(i)
                break
        else:
            result[doc] = [i]
    '''
    return result


def keywords_tfidf(corpus, top_n):
    vectorizer = CountVectorizer()
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(vectorizer.fit_transform(corpus))
    words = vectorizer.get_feature_names()
    key_words = []
    for index in range(len(corpus)):
        row = tfidf.getrow(index).toarray()[0].ravel()
        top_idx = row.argsort()[-top_n:][::-1]
        key_words.append([words[i] for i in top_idx])
    return key_words


def keywords_count(corpus, top_n):
    key_words = []
    for index in range(len(corpus)):
        word_count = {}
        for word in corpus[index].split(" "):
            count = word_count.get(word, 0)
            word_count[word] = count + 1
        top_words = list(sorted(word_count.items(), key=lambda x: x[1], reverse=True))[:top_n]
        key_words.append([w[0] for w in top_words])
    return key_words




if __name__ == '__main__':
    corpus = ["我 来到 北京 清华大学",  # 第一类文本切词后的结果，词之间以空格隔开
               "他 来到 了 网易 杭研 大厦",  # 第二类文本的切词结果
               "小明 硕士 毕业 与 中国 科学院",  # 第三类文本的切词结果
               "我 爱 北京 天安门"]
    r=k_means_tfidf(corpus,1,10)
    print(r)


