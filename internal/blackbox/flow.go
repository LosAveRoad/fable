package blackbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"mychat/internal/dto/im"
)

type CaseResult struct {
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	DurationMS float64 `json:"duration_ms"`
	Error      string  `json:"error,omitempty"`
}
type Report struct {
	RunID      string       `json:"run_id"`
	Target     string       `json:"target"`
	StartedAt  time.Time    `json:"started_at"`
	DurationMS float64      `json:"duration_ms"`
	Passed     bool         `json:"passed"`
	Cases      []CaseResult `json:"cases"`
	Error      string       `json:"error,omitempty"`
}

// Run creates its own users via REST and exercises the selected public ingress.
// It never starts services, accesses infrastructure, or deletes fixture accounts.
func Run(ctx context.Context, cfg Config) (report Report, err error) {
	start := time.Now()
	report.StartedAt = start.UTC()
	c, err := newClient(cfg)
	if err != nil {
		report.Error = err.Error()
		return report, err
	}
	defer c.close()
	cfg = c.config
	report.Target = cfg.BaseURL
	report.RunID = cfg.RunID
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	defer func() { report.DurationMS = float64(time.Since(start)) / float64(time.Millisecond) }()
	var users [3]identity
	var a, b, third *socket
	var session, group string
	directIDs := map[string]bool{}
	groupIDs := map[string]bool{}
	var sockets []*socket
	defer func() {
		for _, s := range sockets {
			s.close()
			<-s.done
		}
	}()
	connect := func(u identity) (*socket, error) {
		s, e := c.connect(ctx, u)
		if e == nil {
			sockets = append(sockets, s)
		}
		return s, e
	}
	send := func(step context.Context, s, peer *socket, target string, kind im.ReceiveType, content string) (im.SendResultFrame, error) {
		f, e := s.send(step, sendFrame(target, kind, content))
		if e != nil {
			return f, e
		}
		if e = stored(f); e != nil {
			return f, e
		}
		if peer != nil {
			if _, e = peer.waitMessage(step, f.Result.Message.MessageID); e != nil {
				return f, e
			}
		}
		return f, nil
	}
	cases := []struct {
		name string
		run  func(context.Context) error
	}{
		{"REST register login and own profile", func(step context.Context) error {
			for i := range users {
				u, e := c.register(step)
				if e != nil {
					return e
				}
				users[i] = u
				var profile struct {
					UUID     string `json:"uuid"`
					Nickname string `json:"nickname"`
				}
				if e = c.ok(step, "POST", "/user/getUserInfo", u.token, map[string]string{"uuid": u.id}, &profile); e != nil {
					return e
				}
				if profile.UUID != u.id || profile.Nickname != u.nickname {
					return errors.New("own profile identity mismatch")
				}
			}
			return nil
		}},
		{"HTTP and WebSocket authentication and protocol denial", func(step context.Context) error {
			for _, token := range []string{"", tamper(users[0].token)} {
				status, e := c.request(step, "POST", "/user/getUserInfo", token, map[string]string{"uuid": users[0].id}, nil)
				if e != nil {
					return e
				}
				if status != 401 {
					return errors.New("unauthorized HTTP request accepted")
				}
				s, status, e := c.dial(step, token, "v2", c.base.String())
				if s != nil {
					s.close()
					<-s.done
				}
				if e == nil || status != 401 {
					return errors.New("unauthorized WebSocket accepted")
				}
			}
			for _, in := range []struct {
				protocol, origin string
				status           int
			}{{"v999", c.base.String(), 400}, {"v2", "https://invalid.invalid", 403}} {
				s, status, e := c.dial(step, users[0].token, in.protocol, in.origin)
				if s != nil {
					s.close()
					<-s.done
				}
				if e == nil || status != in.status {
					return errors.New("invalid protocol or origin accepted")
				}
			}
			return nil
		}},
		{"Direct session and distinct public v2 sockets", func(step context.Context) error {
			var e error
			session, e = c.open(step, users[0], users[1])
			if e != nil {
				return e
			}
			a, e = connect(users[0])
			if e != nil {
				return e
			}
			b, e = connect(users[1])
			return e
		}},
		{"Bidirectional authoritative receipts and live peer push", func(step context.Context) error {
			for i := 0; i < 2; i++ {
				sender, peer, target := a, b, users[1].id
				if i == 1 {
					sender, peer, target = b, a, users[0].id
				}
				f, e := send(step, sender, peer, target, im.ReceiveTypeUser, "blackbox direct")
				if e != nil {
					return e
				}
				if f.Result.Message.SendID != users[i].id || f.Result.Message.ReceiveID != target || f.Result.Message.ConversationID != "direct:"+session {
					return errors.New("direct receipt identity mismatch")
				}
				directIDs[f.Result.Message.MessageID] = true
			}
			return nil
		}},
		{"Stable ID retry conflict and invalid input preserve connection", func(step context.Context) error {
			input := sendFrame(users[1].id, im.ReceiveTypeUser, "blackbox retry")
			first, e := a.send(step, input)
			if e != nil {
				return e
			}
			if e = stored(first); e != nil {
				return e
			}
			if _, e = b.waitMessage(step, first.Result.Message.MessageID); e != nil {
				return e
			}
			retry, e := a.send(step, input)
			if e != nil {
				return e
			}
			if e = stored(retry); e != nil {
				return e
			}
			if !retry.Result.Replayed || retry.Result.Message.MessageID != first.Result.Message.MessageID {
				return errors.New("retry did not return the same saved message")
			}
			directIDs[first.Result.Message.MessageID] = true
			input.Content = "blackbox changed"
			conflict, e := a.send(step, input)
			if e != nil {
				return e
			}
			if conflict.Error == nil || conflict.Error.Code != im.ErrorIdempotencyConflict {
				return errors.New("changed same-ID input was not rejected")
			}
			invalid := sendFrame(users[1].id, im.ReceiveTypeUser, "")
			out, e := a.send(step, invalid)
			if e != nil {
				return e
			}
			if out.Error == nil || out.Error.Code != im.ErrorInvalidArgument {
				return errors.New("empty input was not rejected")
			}
			valid, e := send(step, a, b, users[1].id, im.ReceiveTypeUser, "blackbox after invalid")
			if e == nil {
				directIDs[valid.Result.Message.MessageID] = true
			}
			return e
		}},
		{"Offline receiver reconnect and authoritative paginated sync", func(step context.Context) error {
			b.close()
			select {
			case <-b.done:
			case <-step.Done():
				return errors.New("receiver close timeout")
			}
			f, e := send(step, a, nil, users[1].id, im.ReceiveTypeUser, "blackbox offline")
			if e != nil {
				return e
			}
			directIDs[f.Result.Message.MessageID] = true
			b, e = connect(users[1])
			if e != nil {
				return e
			}
			history, e := c.sync(step, users[1], "direct:"+session, 2)
			if e != nil {
				return e
			}
			return requireMessages(history, directIDs)
		}},
		{"Same-user new connection supersedes old and sends", func(step context.Context) error {
			old := a
			var e error
			a, e = connect(users[0])
			if e != nil {
				return e
			}
			select {
			case <-old.done:
			case <-step.Done():
				return errors.New("old connection was not superseded")
			}
			f, e := send(step, a, b, users[1].id, im.ReceiveTypeUser, "blackbox after replacement")
			if e == nil {
				directIDs[f.Result.Message.MessageID] = true
			}
			return e
		}},
		{"Group REST create nonmember denial join and three-user fanout", func(step context.Context) error {
			var out struct {
				ID string `json:"group_id"`
			}
			if e := c.ok(step, "POST", "/group/create", users[0].token, map[string]any{"name": "bb-" + cfg.RunID[:min(12, len(cfg.RunID))], "add_mode": 0}, &out); e != nil {
				return e
			}
			group = out.ID
			if group == "" {
				return errors.New("missing group identity")
			}
			var e error
			third, e = connect(users[2])
			if e != nil {
				return e
			}
			denied, e := third.send(step, sendFrame(group, im.ReceiveTypeGroup, "blackbox nonmember"))
			if e != nil {
				return e
			}
			if denied.Error == nil || denied.Error.Code != im.ErrorPermissionDenied {
				return errors.New("nonmember group send accepted")
			}
			status, e := c.request(step, "GET", "/api/v2/messages/sync?"+url.Values{"conversation_id": {"group:" + group}}.Encode(), users[2].token, nil, nil)
			if e != nil {
				return e
			}
			if status != 403 {
				return errors.New("nonmember group history accepted")
			}
			for i := 1; i < 3; i++ {
				if e = c.ok(step, "POST", "/group/join", users[i].token, map[string]string{"group_id": group}, nil); e != nil {
					return e
				}
			}
			f, e := send(step, a, b, group, im.ReceiveTypeGroup, "blackbox group")
			if e != nil {
				return e
			}
			id := f.Result.Message.MessageID
			for _, s := range []*socket{a, third} {
				if _, e = s.waitMessage(step, id); e != nil {
					return e
				}
			}
			groupIDs[id] = true
			return nil
		}},
		{"MCP four tools whitelist deny allow AI push revoke", func(step context.Context) error { return runMCP(step, c, users[0], session, a, b, directIDs) }},
		{"Authoritative history pagination deduplicates retry push and sync", func(step context.Context) error {
			history, e := c.sync(step, users[0], "direct:"+session, 2)
			if e != nil {
				return e
			}
			if e = requireMessages(history, directIDs); e != nil {
				return e
			}
			history, e = c.sync(step, users[2], "group:"+group, 1)
			if e != nil {
				return e
			}
			return requireMessages(history, groupIDs)
		}},
	}
	for _, test := range cases {
		if err != nil {
			report.Cases = append(report.Cases, CaseResult{Name: test.name, Status: "skipped"})
			continue
		}
		stepStart := time.Now()
		step, stop := context.WithTimeout(ctx, cfg.StepTimeout)
		e := test.run(step)
		stop()
		result := CaseResult{Name: test.name, Status: "passed", DurationMS: float64(time.Since(stepStart)) / float64(time.Millisecond)}
		if e != nil {
			result.Status = "failed"
			result.Error = e.Error()
			err = fmt.Errorf("case %s failed: %w", test.name, e)
		}
		report.Cases = append(report.Cases, result)
	}
	report.Passed = err == nil
	return report, err
}
func tamper(token string) string {
	if len(token) < 3 {
		return "invalid"
	}
	i := len(token) - 3
	b := byte('A')
	if token[i] == 'A' {
		b = 'B'
	}
	return token[:i] + string(b) + token[i+1:]
}
func requireMessages(history []im.Message, want map[string]bool) error {
	found := map[string]bool{}
	for _, m := range history {
		found[m.MessageID] = true
	}
	if len(found) != len(want) {
		return errors.New("authoritative message count mismatch")
	}
	for id := range want {
		if !found[id] {
			return errors.New("saved message absent from authoritative history")
		}
	}
	return nil
}

type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}
func runMCP(ctx context.Context, c *client, u identity, session string, a, b *socket, ids map[string]bool) error {
	hc := *c.http
	hc.Transport = bearerTransport{c.transport, u.token}
	cl := mcp.NewClient(&mcp.Implementation{Name: "fable-blackbox", Version: "1"}, nil)
	conn, e := cl.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: c.base.String() + "/mcp", HTTPClient: &hc, DisableStandaloneSSE: true}, nil)
	if e != nil {
		return errors.New("MCP connect failed")
	}
	defer conn.Close()
	tools, e := conn.ListTools(ctx, &mcp.ListToolsParams{})
	if e != nil {
		return errors.New("MCP tool discovery failed")
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, name := range []string{"list_sessions", "get_recent_messages", "search_messages", "send_message"} {
		if !names[name] {
			return errors.New("MCP required tool absent")
		}
	}
	if len(names) != 4 {
		return errors.New("MCP unexpected tool surface")
	}
	call := func(name string, args map[string]any, denied bool) (json.RawMessage, error) {
		result, err := conn.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			return nil, errors.New("MCP tool transport failed")
		}
		if result.IsError != denied {
			return nil, errors.New("MCP tool authorization/result mismatch")
		}
		if denied {
			return nil, nil
		}
		raw, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return nil, errors.New("MCP structured output invalid")
		}
		return raw, nil
	}
	deny := func() error {
		for _, name := range []string{"get_recent_messages", "search_messages", "send_message"} {
			args := map[string]any{"session_uuid": session}
			if name == "search_messages" {
				args["query"] = "blackbox"
			}
			if name == "send_message" {
				args["content"] = "blackbox denied"
			}
			if _, e := call(name, args, true); e != nil {
				return e
			}
		}
		return nil
	}
	if e = deny(); e != nil {
		return e
	}
	raw, e := call("list_sessions", map[string]any{}, false)
	if e != nil {
		return e
	}
	var listed struct {
		Sessions []struct {
			ID string `json:"session_uuid"`
		} `json:"sessions"`
	}
	if json.Unmarshal(raw, &listed) != nil || len(listed.Sessions) != 0 {
		return errors.New("MCP whitelist exposed a denied session")
	}
	if e = c.ok(ctx, "PUT", "/api/v1/ai/setting", u.token, map[string]any{"allowed_session_uuids": []string{session}}, nil); e != nil {
		return e
	}
	raw, e = call("list_sessions", map[string]any{}, false)
	if e != nil {
		return e
	}
	if json.Unmarshal(raw, &listed) != nil || len(listed.Sessions) != 1 || listed.Sessions[0].ID != session {
		return errors.New("MCP allowed session missing")
	}
	for _, name := range []string{"get_recent_messages", "search_messages"} {
		args := map[string]any{"session_uuid": session, "limit": 100}
		if name == "search_messages" {
			args["query"] = "blackbox"
			args["limit"] = 50
		}
		raw, e = call(name, args, false)
		if e != nil {
			return e
		}
		var out struct {
			Messages []struct {
				ID string `json:"message_uuid"`
			} `json:"messages"`
		}
		if json.Unmarshal(raw, &out) != nil || len(out.Messages) == 0 {
			return errors.New("MCP query missed saved messages")
		}
		for _, m := range out.Messages {
			if !ids[m.ID] {
				return errors.New("MCP query returned unrelated message")
			}
		}
	}
	raw, e = call("send_message", map[string]any{"session_uuid": session, "client_message_id": sendFrame("", im.ReceiveTypeUser, "").ClientMessageID, "content": "blackbox AI"}, false)
	if e != nil {
		return e
	}
	var out struct {
		Result *im.SendResult `json:"result"`
	}
	if json.Unmarshal(raw, &out) != nil || out.Result == nil || !out.Result.Stored || out.Result.Message.Origin != im.MessageOriginAI {
		return errors.New("MCP AI save receipt missing")
	}
	id := out.Result.Message.MessageID
	if id == "" {
		return errors.New("MCP AI message identity missing")
	}
	for _, s := range []*socket{a, b} {
		o, e := s.waitMessage(ctx, id)
		if e != nil {
			return e
		}
		if o.message.Origin != im.MessageOriginAI {
			return errors.New("MCP AI push origin mismatch")
		}
	}
	ids[id] = true
	if e = c.ok(ctx, "PUT", "/api/v1/ai/setting", u.token, map[string]any{"allowed_session_uuids": []string{}}, nil); e != nil {
		return e
	}
	return deny()
}
