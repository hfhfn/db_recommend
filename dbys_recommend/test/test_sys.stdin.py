
# print('根据两点坐标计算直线斜率k，截距b：')
# while True:
#     line = input()
#     if line == '\n': break
#
#     x1, y1, x2, y2 = (float(x) for x in line.split())
#     k = (y2 - y1) / (x2 - x1)
#     b = y1 - k * x1
#     print('斜率:{}，截距:{}'.format(k, b))



# import sys
# sys.stdout.write('根据两点坐标计算直线斜率k，截距b：\n')
# while True:
#     line = sys.stdin.readline()
#     if line == '\n': break
#
#     x1, y1, x2, y2 = (float(x) for x in line.split())
#     k = (y2 - y1) / (x2 - x1)
#     b = y1 - k * x1
#     sys.stdout.write('斜率:{}，截距:{}\n'.format(k, b))


# import sys
# sys.stdout.write('根据两点坐标计算直线斜率k，截距b：\n')
# for line in sys.stdin:
#     if line == '\n': break
#
#     x1, y1, x2, y2 = (float(x) for x in line.split())
#     k = (y2 - y1) / (x2 - x1)
#     b = y1 - k * x1
#     sys.stdout.write('斜率:{}，截距:{}\n'.format(k, b))


import sys
import json

user_set = set()
click_cnt = 0
total_time = 0
expose_cnt = 0
for line in sys.stdin:
    labels = line.split("\t")[0]
    user_info = line.split("\t")[1]
    uid = json.loads(user_info).get("uid")
    user_set.add(uid)

    expose_cnt = expose_cnt + 1 if int(labels.split("#")[1]) in [0,1,2] else expose_cnt
    click_cnt = click_cnt + 1 if int(labels.split("#")[1]) in [1,2] else click_cnt
    total_time = total_time + int(labels.split("#")[2])

print(f"正样本:{click_cnt},总曝光:{expose_cnt},曝光点击率:{click_cnt/expose_cnt}")
print(f"总时长:{total_time},总用户:{len(user_set)},人均观看时长:{total_time/len(user_set)}")
