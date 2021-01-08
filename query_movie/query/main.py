from setting.conf import cut_field
from query.cons_calculate import similar_calculate, generate_eval
from query.pre_query import PreProcessData
from query.ready_model import TestReady, TrainReady


class CheckServicer(object):
    """
    构建文本查重类
    """
    def __init__(self):
        # 预加载模型
        self.train_tfidf, self.train_frame = TrainReady().pre_train()
        self.tfidf_model = TestReady().pre_test()

    def CheckArticle(self, movie_id, content):
        """
        :param article_id: str/[str..]
        :param content: str/[str..]
        :return: {}
        """
        # 构建预处理语料类对象
        prepro = PreProcessData(movie_id=movie_id, content=content)
        # 分词
        test_frame = prepro.cut_text()
        # print(test_frame.head())
        # 计算tfidf权重
        # print('计算测试数据tfidf....')
        input_list = test_frame[cut_field].tolist()
        # 调用transform
        test_tfidf = self.tfidf_model.transform(input_list)
        # print('计算完成')
        # 相似度计算
        sort_list = similar_calculate(self.train_tfidf, test_tfidf)
        # 生成结果
        result = generate_eval(sort_list, test_frame, self.train_frame)
        return result



if __name__ == '__main__':

    content = """["欲望都市I.Sex.And.The.City.1.2008.BluRay.720p.x264.AC3-CMCT.mkv", \
               "[BD影视分享bd-film.cc]怒火救援.Man.on.Fire.2004.全屏版.DDP5.1.HD1080P.国英双语.中英双字.mkv", \
               "[BD影视分享bd-film.cc]终结者：黑暗命运.Terminator.Dark.Fate.2019.BD1080P.中英双字", \
               "[变形金刚5：最后的骑士].Transformers.The.Last.Knight.2017.3D.BluRay.1080p.HSBS.x264.TrueHD7.1-CMCT", \
               "的士速递3..720p.BD国语中英双字[最新电影www.66ys.tv]"]"""
    id = """[0,1,2,3,4]"""
    import time
    start = time.time()
    result = CheckServicer().CheckArticle(id, content)
    print(result)
    end = time.time()
    spend = end - start
    print(spend)