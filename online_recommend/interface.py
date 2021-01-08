from flask import Flask, request, jsonify, make_response
import requests
from flask_cors import cross_origin
from pymysql import connect


def con_mysql(find_name):
    # 创建Connection连接
    conn = connect(host='mysql',port=3306,user='root',password='mysql',database='recommend',charset='utf8')
    # 获得Cursor对象
    csl = conn.cursor()

    # 安全的方式
    # 构造参数列表
    params = find_name  # list
    try:
        # 执行select语句，并返回受影响的行数：查询所有数据
        if len(params[0]) > 10:
            count = csl.execute("select movie_id from user_recall_1969 where user_id=%s", params)
        else:
            csl.execute("select movie_id2 from movie_recall_1969 where movie_id=%s", params)
        # 打印受影响的行数
        # print(count)
        # 获取查询的结果
        # result = cs1.fetchone()
        result = csl.fetchall()
        recall = []
        for i in result:
            recall.append(i[0])

        return recall
    except:
        print('传入参数有误')

    # 关闭Cursor对象
    csl.close()
    # 关闭Connection对象
    conn.close()


# 注册flask应用
app = Flask(__name__)
# app.config.from_object(check_server)

@app.route('/', methods=['GET', 'POST', 'OPTIONS'])    # /?uid=''|mid=''
@cross_origin(origins="*", maxAge=3600)  # 跨域装饰器
def register():
    try:
        # uid = request.json['']
        # mid = request.json['']
        uid = request.args.get('uid', None)
        mid = request.args.get('mid', None)
        find_name = []
        if mid is not None:
            # find_name = ['movie_id2', 'movie_recall_1969', 'movie_id', mid]
            find_name = [mid]
        elif uid is not None:
            # find_name = str(['movie_id', 'user_recall_1969', 'user_id', uid])
            find_name = [uid]

        result = con_mysql(find_name)
        d = {
            "recommend": str(result)
        }
        # 组织响应报文
        response = make_response((jsonify(d), 202))
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = \
            'Content-Type, Content-Length, Authorization, Accept, X-Requested-With , yourHeaderFeild'
        response.headers['Content-Type'] = "application/json;charset=utf-8"
        return response
    except requests.exceptions.HTTPError as e:
        return e


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True, use_reloader=False, threaded=True)