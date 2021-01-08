import pandas as pd
import re
from utils import cut
from tqdm import tqdm
from setting.conf import content_field, cut_field


class PreProcessData(object):
    """
    对数据进行清洗，分词等操作
    """
    def __init__(self, movie_id, content):
        """
        :param data: pd.dataframe
        :param cut_field: str
        :param article_id: str
        """
        self.movie_id = movie_id
        self.content = content
        self.content_field = content_field
        self.cut_field = cut_field

    @staticmethod
    def tokenlize(sentence):
        # filters = ['!', '"', '#', '$', '%', '&', '\(', '\)', '\*', '\+', ',', '-', '\.', '/', ':', ';', '<', '=', '>',
        #             '\?', '@', '\[', '\\', '\]', '^', '_', '`', '\{', '\|', '\}', '~', '\t', '\n', '\x97', '\x96', '”', '“', ]
        filters = ['\.', '中英', '双字', '国英', '双语', 'BD', '720P', '1080P', '3D', 'tv', 'mkv', '最新', '电影',
                   '全屏版', '国语', 'www']
        # sentence = sentence.lower() #把大写转化为小写
        # sentence = re.sub("<br />"," ",sentence)
        # sentence = re.sub("I'm","I am",sentence)
        # sentence = re.sub("isn't","is not",sentence)
        result = re.sub("|".join(filters), " ", sentence)
        # result = [i for i in sentence.split(" ") if len(i)>0]

        return result

    def cleaning_data(self):
        """
        对录入数据进行清洗
        :return: self
        """
        data = []
        # 判断是否是list
        try:
            movie_id = eval(self.movie_id)
            query = eval(self.content)
            # if isinstance(movie_id, list):
            for i in range(len(movie_id)):
                data.append([movie_id[i], query[i]])
        except:
            data.append([self.movie_id, self.content])

        col_lis = ['movie_id', 'content']
        # 组织成dateframe格式数据
        df = pd.DataFrame(data, columns=col_lis)
        # 正则过滤选择所有中文、英文、数字字符， re.S: 跨行匹配, 忽略\n 的分隔（根据需求修改）
        pat = re.compile('[\u4e00-\u9fa5a-zA-Z0-9]+', re.S)
        text_list = []
        bar = df[self.content_field].tolist()
        total = len(df)
        for text in tqdm(bar, total=total, desc='清洗数据'):
            text = ' '.join(pat.findall(text))
            text = self.tokenlize(text)
            text_list.append(text)
        # 清洗后的数据插入dataframe
        df[self.cut_field] = text_list
        # self.data = self.data.dropna()
        return df

    def cut_text(self):
        """
        对content字段做分词处理
        :param: self.data
        cut_data路径文件不存在时, 调用 pre_input()方法，需要先执行 cleaning_data()方法得到 self.data
        :return: pd.dataframe
        """
        df = self.cleaning_data()
        cut_frame = df
        sent_list = []
        bar = cut_frame[self.cut_field].tolist()
        total = len(cut_frame)
        for sent in tqdm(bar, total=total, desc='分词'):
            # 调用 utils.cut 方法进行分词
            words = cut(sent)
            words = ' '.join(words)
            if len(words) == 0:
                words = sent
            sent_list.append(words)
        # 把分词结果再组织成dataframe
        cut_frame[self.cut_field] = sent_list
        # cut_frame = cut_frame.dropna()
        return cut_frame

