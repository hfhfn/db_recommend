import os
import codecs


def merge():
    dir_path = os.path.dirname(os.path.abspath(__name__))
    # print(dir_path)
    file_path = [i for i in os.listdir(dir_path) if i.endswith('.txt')]
    # print(file_path)
    all_list = []
    for temp in file_path:
        all_list.extend(codecs.open(temp).readlines())
    all_list = sorted([i.strip() for i in set(all_list) if len(i) <= 5], reverse=True)
    with open("all_stopwords.txt", "a") as f:
        for t in all_list:
            f.write(t + "\n")


if __name__ == '__main__':
    merge()
