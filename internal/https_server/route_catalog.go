package https_server

import (
	v1 "mychat/internal/api/v1"
	"mychat/internal/dto/im"
	"mychat/internal/dto/request"
	"mychat/internal/dto/response"
	"mychat/internal/httpdocs"
	"mychat/internal/service/gormservice"

	"github.com/gin-gonic/gin"
)

// legacyRoutes is both registration and OpenAPI source. Status annotations
// describe existing controller branches, including historical raw error JSON.
func legacyRoutes() []httpdocs.Route {
	errorBody := httpdocs.ErrorBody{}
	raw := httpdocs.RawError{}
	result := func(status int, body any) httpdocs.Response {
		return httpdocs.Response{Status: status, Bodies: []any{body}}
	}
	empty := func(status int) httpdocs.Response { return httpdocs.Response{Status: status} }
	errors := func(statuses ...int) []httpdocs.Response {
		r := []httpdocs.Response{}
		for _, s := range statuses {
			r = append(r, result(s, errorBody))
		}
		return r
	}
	route := func(method, path, id, summary string, auth bool, handler gin.HandlerFunc, req any, ok httpdocs.Response, failures ...httpdocs.Response) httpdocs.Route {
		return httpdocs.Route{Method: method, Path: path, ID: id, Summary: summary, Authenticated: auth, Handler: handler, Request: req, Responses: append([]httpdocs.Response{ok}, failures...)}
	}
	return []httpdocs.Route{
		route("POST", "/register", "register", "Register a user", false, v1.Register, request.RegisterRequest{}, result(202, response.RegisterResponse{}), httpdocs.Response{Status: 400, Bodies: []any{errorBody, raw}}, result(409, raw), result(502, raw)),
		route("POST", "/login", "login", "Authenticate a user", false, v1.Login, request.LoginRequest{}, result(200, response.LoginResponse{}), result(400, raw), result(500, raw)),
		route("POST", "/user/getUserInfo", "getUserInfo", "Read user information", true, v1.GetUserInfo, request.GetUserInfoRequest{}, result(200, response.GetUserInfoResponse{}), result(400, raw), httpdocs.Response{Status: 403, Bodies: []any{raw, ""}}, result(409, raw), result(500, raw)),
		route("POST", "/session/openSession", "openSession", "Open a direct session", true, v1.OpenSession, request.OpenSessionRequest{}, result(200, response.OpenSessionResponse{}), result(400, raw), result(403, ""), result(404, raw), httpdocs.Response{Status: 500, Bodies: []any{raw, ""}}),
		route("POST", "/session/getUserSessionList", "getUserSessionList", "List direct sessions", true, v1.GetUserSessionList, nil, result(200, []response.UserSessionListResponse{}), result(400, raw), httpdocs.Response{Status: 500, Bodies: []any{raw, errorBody}}),
		route("POST", "/message/getMessageList", "getMessageList", "Read legacy direct message history as a bare array", true, v1.GetMessageList, request.GetMessageListRequest{}, result(200, []response.MessageResponse{}), errors(400, 403, 500)...),
		route("POST", "/group/create", "createGroup", "Create a group", true, v1.CreateGroup, request.CreateGroupRequest{}, result(201, gormservice.GroupResponse{}), errors(400, 404, 500)...),
		route("POST", "/group/join", "joinGroup", "Join a group; 202 means approval is pending", true, v1.JoinGroup, request.GroupIDRequest{}, empty(204), errors(202, 400, 403, 404, 500)...),
		route("POST", "/group/join/approve", "approveGroupJoin", "Approve group membership", true, v1.ApproveGroupJoin, request.ApproveGroupJoinRequest{}, empty(204), errors(400, 403, 404, 500)...),
		route("POST", "/group/leave", "leaveGroup", "Leave a group", true, v1.LeaveGroup, request.GroupIDRequest{}, empty(204), errors(202, 400, 403, 404, 500)...),
		route("POST", "/group/info", "getGroupInfo", "Read group information", true, v1.GetGroupInfo, request.GroupIDRequest{}, result(200, gormservice.GroupResponse{}), errors(400, 404, 500)...),
		route("POST", "/group/list/joined", "getJoinedGroups", "List joined groups", true, v1.GetJoinedGroupList, nil, result(200, []gormservice.GroupResponse{}), errors(500)...),
		route("POST", "/group/list/owned", "getOwnedGroups", "List owned groups", true, v1.GetOwnedGroupList, nil, result(200, []gormservice.GroupResponse{}), errors(500)...),
		route("POST", "/group/message/list", "getGroupMessageList", "Read group message history as a bare array", true, v1.GetGroupMessageList, request.GroupIDRequest{}, result(200, []response.MessageResponse{}), errors(400, 403, 404, 500)...),
		route("POST", "/session/getGroupSessionList", "getGroupSessionList", "List group sessions", true, v1.GetGroupSessionList, nil, result(200, []response.UserSessionListResponse{}), errors(500)...),
		route("POST", "/contact/list/users", "getContacts", "List contact users", true, v1.GetContactUserList, nil, result(200, []response.GetUserInfoResponse{}), errors(500)...),
		route("GET", "/api/v1/ai/setting", "getAISetting", "Read AI session access settings", true, v1.GetAISetting, nil, result(200, response.AISettingResponse{}), errors(400, 403, 500)...),
		func() httpdocs.Route {
			r := route("PUT", "/api/v1/ai/setting", "changeAISetting", "Replace AI session access settings; an empty list revokes all access", true, v1.ChangeAISetting, request.ChangeAISettingRequest{}, result(200, response.AISettingResponse{}), errors(400, 403, 500)...)
			r.RequiredFields = []string{"allowed_session_uuids"}
			return r
		}(),
	}
}

// LogicRoutes supplies the same business operations mounted by NewLogicEngine.
// The caller supplies the actual sync adapter; documentation needs no live DB.
func LogicRoutes(sync gin.HandlerFunc) []httpdocs.Route {
	routes := legacyRoutes()
	return append(routes, httpdocs.Route{
		Method: "GET", Path: "/api/v2/messages/sync", ID: "syncMessages", Summary: "Synchronize an authorized conversation using a bounded cursor page", Authenticated: true, Handler: sync, Query: im.SyncQuery{},
		QueryRules: map[string]map[string]any{
			"conversation_id": {"required": true, "description": "Authorized direct:<session_uuid> or group:<group_uuid> conversation"},
			"cursor":          {"description": "Opaque cursor bound to this conversation; omitted on the first page"},
			"limit":           {"minimum": 0, "maximum": 500, "default": 100, "description": "Omitted or zero uses 100; otherwise 1–500"},
		},
		Responses: []httpdocs.Response{
			{Status: 200, Bodies: []any{im.SyncPage{}}},
			{Status: 400, Description: "Invalid query, limit, cursor, or conversation", Bodies: []any{httpdocs.ErrorBody{}}},
			{Status: 403, Description: "No access to this session or group", Bodies: []any{httpdocs.ErrorBody{}}},
			{Status: 503, Description: "Database or dependency unavailable", Bodies: []any{httpdocs.ErrorBody{}}},
		},
	})
}

func mount(r *gin.Engine, jwtKey []byte, routes []httpdocs.Route) {
	for _, route := range routes {
		if route.Handler == nil {
			panic("nil HTTP handler for " + route.Path)
		}
		if route.Authenticated {
			r.Handle(route.Method, route.Path, Auth(jwtKey), route.Handler)
		} else {
			r.Handle(route.Method, route.Path, route.Handler)
		}
	}
}
