"""
实现文本的分词模型
"""
import re

import jieba
import logging

# 关闭jieba日志
jieba.setLogLevel(logging.INFO)


def cut(sentence):
    """
    按照词语进行分词
    :param sentence:str
    :return: 【str,str,str】
    """
    temp = []
    sentence = str(sentence).lower()
    result = jieba.lcut(sentence)
    # pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*|[-+]?\.?[0-9]\d*$')  # 匹配所有数字
    # pattern = re.compile(r'^[a-zA-Z0-9]+$')
    number = re.compile(r'^\d+$')
    alpha = re.compile(r'^[a-zA-Z]+$')
    alnum = re.compile(r'^[a-zA-Z\d]+$')
    for i in result:
        word = i.strip()
        # isdigit, isalnum, 判断方式不靠谱
        result = number.match(word)
        if result:
            temp.append(word)
            continue
        result2 = alpha.match(word)
        if result2:
            temp.append(word)
            continue
        result3 = alnum.match(word)
        if result3:
            continue
        if len(word) > 1:
            temp.append(word)

    return temp

