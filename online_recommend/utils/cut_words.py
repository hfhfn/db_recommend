from utils.ready_dict import userDict_path, get_stopwords_list, tmpDict_path, originDict_path


class BaseCutWords(object):
    """
    传入不同的需要处理的dataframe需要重写segmentation中的字段
    """
    # 关闭jieba日志
    import jieba
    import logging
    jieba.setLogLevel(logging.INFO)
    # 手动初始化（可选）在0.28之前的版本是不能指定主词典的路径的，有了延迟加载机制后，你可以改变主词典的路径:
    # jieba.initialize()
    # jieba.set_dictionary(originDict_path)
    # 停用词列表
    # 停用词没有停用数字 0 1 2 3 4 5 6 7 8 9， 保留数字用来分辨电影集数
    stopwords_list = get_stopwords_list()
    # 在需要的父类中，添加用户词典，基类不添加
    # jieba.load_userdict(userDict_path)

    def __init__(self, cate_id=1969, filter=True, stopwords=True):
        self.filter = filter
        self.stopwords = stopwords
        self.cate_id = cate_id

    def cut_sentence(self, sentence):
        # 先分词
        import jieba.posseg as pseg
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        # print(sentence,"*"*100)
        # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
        filtered_words_list = []
        # 原语句长度小于等于2，则不用分词，直接加入分词列表
        if len(sentence) <= 2:
            filtered_words_list.append(sentence)
        else:
            seg_list = pseg.lcut(sentence)
            if self.stopwords:
                seg_list = [i for i in seg_list if i.word not in self.stopwords_list]
            if self.filter:
                for seg in seg_list:
                    # print(seg)
                    # 把所有数字都 加入分词中，包括flag标记为'm'的还有标记为'x'的
                    if seg.word.isdigit():
                        filtered_words_list.append(seg.word)
                        continue
                    # 数词包含数字以及很多跟数量相关的词，都加入分词
                    if seg.flag.startswith("m"):  # 所有数词开头的
                        filtered_words_list.append(seg.word)
                    # 小于等于一个字符的 去除
                    elif len(seg.word) <= 1:
                        continue
                    elif seg.flag == "eng":  # 是英文
                        # 英文小于等于2个字符的去除
                        if len(seg.word) <= 2:
                            continue
                        else:
                            # 英文都转换成小写 加入分词
                            tmp_word = seg.word.lower()
                            filtered_words_list.append(tmp_word)
                    elif seg.flag.startswith("n"):  # 所有名词开头的
                        filtered_words_list.append(seg.word)
                    elif seg.flag.startswith("v"):  # 所有动词开头的
                        filtered_words_list.append(seg.word)
                    elif seg.flag.startswith("a"):  # 所有形容词开头的
                        filtered_words_list.append(seg.word)
                    # 用户自定义词典格式： 每个词一行，分3列，空格隔开， 词word，词频freq（选参），词性tag（选参）
                    # 自定义词典如果不带词性， 则默认归为 'un' 词性
                    # 'x' 未知词/符号,  不能识别的数字和符号或者英文和汉字都会属于 'x' 词性
                    elif seg.flag in ["un", "x", "i", "j", "l", "t", "tg"]:  # 是自定义/未知, 不能识别的，成语，简称，习用语，时间词
                        filtered_words_list.append(seg.word)
            else:
                for seg in seg_list:
                    filtered_words_list.append(seg.word)

        return filtered_words_list

    def segmentation(self, partitions):
        for row in partitions:
            yield row.id, row.cid, row.title, self.cut_sentence(row.summary), row.create_time

    def get_words(self, sentence_df=None):
        """
        返回分词结果
        :return: 【dataframe】
        """
        doc = sentence_df.rdd.mapPartitions(self.segmentation)
        doc = doc.toDF(["movie_id", "cate_id", "title", "words", "create_time"])
        # print(doc.show())
        return doc


class MovieCutWords(BaseCutWords):
    # 结巴加载用户词典
    import jieba
    jieba.load_userdict(userDict_path)
    # 所有的停用词列表
    # stopwords_list = get_stopwords_list()
    jieba.enable_parallel(4)


class FilterCutWords(BaseCutWords):
    """
    重写分词类方法，分词用来过滤召回电影名相同的电影
    """
    # 结巴加载用户词典
    import jieba
    # jieba.load_userdict(tmpDict_path)
    # 所有的停用词列表
    # stopwords_list = get_stopwords_list()
    jieba.enable_parallel(4)  # 开启并行分词模式，参数为并行进程数
    # jieba.disable_parallel()  # 关闭并行分词模式


    def segmentation(self, partitions):
        """
        # 下面列表中的电影是标题被分词后为空的，后续处理会留下它们
        ['心若在，家就在', '你会在那里吗？', '简·爱', '爱，很美', '倮·恋', '《龙在哪里？》',
         '我不穷，我只是没钱', '为什么是他？', '影·享', '破·局', '放·逐', '追·踪',
         '在…这里', '我-活过', '谁是G?', '祂都不爱我?', '缘来：是你', '没有你·没有我',
         '雪·葬', '零-zero', '与此同时', '血，总是热的', '练·恋·舞', '喂水开了没?',
         '今天的鱼怎么样?', '龙在哪里？', '欢·爱', '我们的存在（上）', '虎！虎！虎！', 'Cin(T)a',
         '一、二、三，现在', '更大，更强，更快', '熊,熊！', '“吃吃”的爱', '爸，我一定行的',
         '这里,那里', '夜·店', '她们,他们', '喊·山', '盲·道', '嘿，蠢贼', '雾人(前传)', '毒。诫']
        :param partitions:
        :return:
        """
        # 以下列表中的电影是分词为空且重复的或者日语电影或者test数据，选择丢弃。
        if self.cate_id == 1969:
            x_list = ['a2-b-c', '一、二、三,现在!', '一、二、三，现在！', '我们的存在（下）', 'スターフィッシュホテル',
                      'さいはてにてやさしい香りと待ちながら', '母の呗がきこえる', '&***()……%￥', '闘牌伝アカギ']
        elif self.cate_id == 1970:
            x_list = ['2018星光盛典荣誉时刻']
        else:
            x_list = []

        for row in partitions:
            title = row.title
            try:
                title_len = len(title)
            except:
                title_len = 0
            if title in x_list:
                words = None
                words_len = None
            # title长度大于2的才分词
            elif title_len > 2:
                # 此处分词可能返回空的列表
                words = self.cut_sentence(title)
                words_len = len(words)
                if words_len == 0:
                    words = [title]
                    words_len = 1
            else:
                words = [title]
                words_len = 1

            # 此处继承的filter参数, 默认为True是非法的，需要传入 'user' or 'movie', 分别表示基于用户和基于影视
            if self.filter == 'user':
                yield row.user_id, row.movie_id, row.cate_id, row.weight, title, \
                      title_len, words, words_len
            else:
                yield row.movie_id, row.movie_id2, row.cos_sim, title, \
                      title_len, words, words_len

    def get_words(self, sentence_df=None):
        """
        返回分词结果
        :return: 【dataframe】
        """
        doc = sentence_df.rdd.mapPartitions(self.segmentation)
        if self.filter == 'user':
            doc = doc.toDF(["user_id", "movie_id", "cate_id", "weight", "title", "title_len", "words", "words_len"])\
                    .dropna()
        else:
            doc = doc.toDF(["movie_id", "movie_id2", "cos_sim", "title", "title_len", "words", "words_len"]).dropna()
        # print(doc.show())
        return doc


# class MovieFilterCutWords(UserFilterCutWords):
#
#     def segmentation(self, partitions):
#         # 以下列表中的电影是分词为空且重复的或者日语电影或者test数据，选择丢弃。
#         x_list = ['a2-b-c', '一、二、三,现在!', '一、二、三，现在！', '我们的存在（下）', 'スターフィッシュホテル',
#                   'さいはてにてやさしい香りと待ちながら', '母の呗がきこえる', '&***()……%￥', '闘牌伝アカギ']
#
#         for row in partitions:
#             title = row.title
#             title_len = len(title)
#             # 标题中有书名号的基本为花絮，去除
#             if title in x_list or '《' in list(title):
#                 words = None
#                 words_len = None
#             # title长度大于2的才分词
#             elif title_len > 2:
#                 # 此处分词可能返回空的列表
#                 words = self.cut_sentence(title)
#                 words_len = len(words)
#             else:
#                 words = [title]
#                 words_len = len(words)
#             yield row.movie_id, row.movie_id2, row.cos_sim, title, \
#                   title_len, words, words_len
#
#     def get_words(self, sentence_df=None):
#         """
#         返回分词结果
#         :return: 【dataframe】
#         """
#         doc = sentence_df.rdd.mapPartitions(self.segmentation).toDF(
#             ["movie_id", "movie_id2", "cos_sim", "title", "title_len", "words", "words_len"]).dropna()
#         # print(doc.show())
#         return doc


# if __name__ == '__main__':
#     pass
