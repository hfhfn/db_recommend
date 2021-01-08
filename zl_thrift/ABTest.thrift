namespace py absystem
namespace cpp absystem

//请求
struct ABTestReq {
	1: string uid,	//用户设备id
	2: map<string, string> params,	//(可选)参数
	3: string ext,	//(可选)拓展字段
}

//响应
struct ABTestRsp {
	1: string status,	//响应状态
	2: string abtest_ids,	//实验id(可能","连接多个)
	3: string abtest_params	//ab参数(json)
}

//接口
service ABTest {
	ABTestRsp get(1: ABTestReq req)	//获取参数
}
