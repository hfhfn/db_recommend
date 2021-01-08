# -*- coding: utf-8 -*-

"""
oss 操作
"""
# import oss2
import os

from ks3.connection import Connection
ak = 'AKLTMh9767gJRn-9bBkcqN9alw'
sk = 'OCAPQO5HmbqM9z7fhdpJZuf+smjb6G4eCC5ArjF+ySw9qgGDpe7mnIb2WNaK+RvHsQ=='
c = Connection(ak, sk, host='ks3-cn-beijing.ksyun.com', is_secure=False, domain_mode=False)

bucket_name = "dangbeisvdsamesize"
# root_path = "/home/hadoop/"
# down_dir = "dsp/dbznds/2017122704"
b = c.get_bucket(bucket_name)
# max_size = 100
# file_size = 0
for k in b.list():
#     try:
#         dir_path = os.path.split(k.name)[0]
#         if down_dir in dir_path:
#             file_size += k.size
#             if file_size / (1024 * 1024) <= max_size:
#                 # print(k.name, k.size, k.last_modified)
#                 file_name = os.path.split(k.name)[1]
#                 if not os.path.exists(root_path + dir_path):
#                     os.makedirs(root_path + dir_path)
#                 # print(dir_path, file_name)
#                 k = b.get_key(k.name)
#                 k.get_contents_to_filename(root_path + "{}".format(k.name))
#                 # # s = k.get_contents_as_string().decode()
#                 # # print(s)
#                 break
#     except:
#         pass #异常处理
    # break

    try:
        with open('./file_name', 'a', encoding='utf-8') as f:
            f.write(k.name + '\n')
    except:
        pass  # 异常处理


    # try:
    #     k = b.get_key(k.name)
    #     if k:
    #          print(k.name, k.size, k.last_modified)
    # except:
    #     pass #异常处理
    # break

# Get file info
#source_path = 'SOURCE_FILE_PATH'
#source_size = os.stat(source_path).st_size

# Create a multipart upload request
# 此处os.path.basename(source_path)可以替换为需要设置的objectKey
#mp = b.initiate_multipart_upload(os.path.basename(source_path), policy="private")
# Use a chunk size of 50 MiB (feel free to change this)
#chunk_size = 52428800
#chunk_count = int(math.ceil(source_size*1.0 / chunk_size*1.0))


