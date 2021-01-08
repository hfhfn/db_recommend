namespace cpp zlyq
namespace py zlyq

struct Item	// 表示单个推荐Item
{
	1: i64 item_id,		// item id
	2: double score,	// 得分
	3: string strategy,	// 推荐策略
	4: optional map<string, string> item_info={},	// 扩展字段
}

// 请求
struct Req
{
	1: i32 uid_type,	// 默认传14即可(代表始终使用udid)
	2: string udid,		// 用户设备id
	3: string app_id,	// 应用id(默认传1,代表ab服务的推荐项目)
	4: i64 channel_id,	// 频道id
	5: i32 start_time,	// (不传)开始时间
	6: i32 end_time,	// (不传)结束时间
	7: i32 count = 40,	// 请求数量
	8: string abtest_parameters='',	// ab测试参数
	9: optional string impression_id='',	// (不传)请求id
	10: optional string context_info='',	// (不传)上下文-明文，json结构(参见Part1样本准备)
	11: optional i64 user_id=0,	// (不传)用户id, 当uid_type=12此字段才有意义
	12: optional string context_feature='',	// 上下文-id化, json格式(参见样本的context-info的key, 保持严格一致, 其中chnid和time必传参数)
	13: optional i32 content_type=0,	// 默认传1
}

// 响应
struct Rsp
{
	1: string status = '',	// 无错误是, status为空，否则为错误信息
	2: list<Item> items,	// 推荐item列表
	3: string abtest_ids,	// 当前请求的实验ID(参加ABTest子系统)
	4: string request_id,	// 请求id
}

service Zreco
{
	Rsp get_item_list(1:Req req),
}






