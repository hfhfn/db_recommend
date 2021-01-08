# -*- coding:utf-8 -*-
import requests
import json


content = """["欲望都市I.Sex.And.The.City.1.2008.BluRay.720p.x264.AC3-CMCT.mkv", \
           "[BD影视分享bd-film.cc]怒火救援.Man.on.Fire.2004.全屏版.DDP5.1.HD1080P.国英双语.中英双字.mkv", \
           "[BD影视分享bd-film.cc]终结者：黑暗命运.Terminator.Dark.Fate.2019.BD1080P.中英双字", \
           "[变形金刚5：最后的骑士].Transformers.The.Last.Knight.2017.3D.BluRay.1080p.HSBS.x264.TrueHD7.1-CMCT", \
           "的士速递3..720p.BD国语中英双字[最新电影www.66ys.tv]"]"""
movie_id = """[0,1,2,3,4]"""

# content = '007 大破杀机'
# movie_id = 1

headers = {'content-type': 'application/json'}
# url = "http://47.93.89.40:8090"  # IP和端口号
url = "http://127.0.0.1:8000/query"  # IP和端口号
# url = "http://python.shangyexinzhi.com/chachong"
data = {
    "movie_id": movie_id,
    "content": content
}
r = requests.post(url, data=json.dumps(data), headers=headers)
# print(r.text)
print(r.json()['response'])
