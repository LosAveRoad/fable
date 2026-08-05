package v1

// GetMessageList 处理 POST /message/getMessageList。
//
// 鉴权：需要 Bearer Token。
//
// 请求 JSON：
//
//	{
//	  "user_one_id": "U001",
//	  "user_two_id": "U002"
//	}
//
// 已认证用户的 uuid 必须属于聊天双方之一。MVP0 只返回文本消息，并按照
// 创建时间和消息 ID 进行稳定排序；正式使用前应补充分页能力。
//
// 成功响应：200 OK。Data 是数组，元素包含 send_id、send_name、
// send_avatar、receive_id、type、content 和 created_at。
//
// 错误响应：400 用户 ID 格式错误；401 未认证；403 请求者不是会话参与者；
// 500 数据持久化失败。

type reqJSON struct {
	user1 string
	user2 string
}
