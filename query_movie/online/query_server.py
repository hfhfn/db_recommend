from flask import Flask, request, jsonify, make_response
import requests
from flask_cors import cross_origin
from query.main import CheckServicer

# 注册flask应用
app = Flask(__name__)
check_server = CheckServicer()
app.config.from_object(check_server)


@app.route('/query', methods=['POST', 'OPTIONS'])
@cross_origin(origins="*", maxAge=3600)  # 跨域装饰器
def register():
    try:
        movie_id = request.json['movie_id']
        content = request.json['content']
        # 调用查重对象方法
        result = check_server.CheckArticle(movie_id, content)
        d = {
            "response": str(result)
        }
        # 组织响应报文
        response = make_response((jsonify(d), 202))
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = \
            'Content-Type, Content-Length, Authorization, Accept, X-Requested-With , yourHeaderFeild'
        response.headers['Content-Type'] = "application/json;charset=utf-8"
        return response
    except requests.exceptions.HTTPError as e:
        return e


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True, use_reloader=False, threaded=True)
