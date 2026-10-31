//go:build integration

package logic_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/datatypes"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/logic"
	"mychat/internal/logic/messagestore"
	"mychat/internal/model"
)

// Removing SQL idempotency or RPC serialization must produce duplicate rows or
// different persisted views; production dispatch is inspected at its broker boundary.
func TestIntegrationLogicDirectConcurrentIdempotency(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	request := f.request(route, f.users[1], " \n ordinary text\t ")
	const n = 12
	start := make(chan struct{})
	results := make(chan *pb.SendResult, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), request)
			errs <- err
			if response != nil {
				results <- response.Result
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *pb.MessageView
	fresh, seen := 0, 0
	for result := range results {
		seen++
		if result == nil || !result.Stored || result.Dispatch != pb.DispatchState_DISPATCH_STATE_QUEUED {
			t.Fatalf("result=%v", result)
		}
		if first == nil {
			first = result.Message
		}
		if !proto.Equal(first, result.Message) {
			t.Fatalf("unstable replay: first=%v replay=%v", first, result.Message)
		}
		if !result.Replayed {
			fresh++
		}
	}
	if seen != n || fresh != 1 || first.Content != request.Content || first.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_USER || first.ConversationId != "direct:"+f.sessions[0] {
		t.Fatalf("seen=%d fresh=%d message=%v", seen, fresh, first)
	}
	f.count(t, 1)
	records := f.publisher.records()
	if len(records) != n {
		t.Fatalf("publish attempts=%d", len(records))
	}
	for _, record := range records {
		task := integrationTask(t, record)
		if task.Message.MessageID != first.MessageId || task.Message.Content != request.Content || !reflect.DeepEqual(task.Recipients, integrationSorted(f.users[0], f.users[1])) || len(task.Targets) != 1 || task.Targets[0].UserID != f.users[0] || task.Targets[0].RouteToken != route.Identity.RouteToken || task.RequestID != "integration-request" {
			t.Fatalf("task=%+v", task)
		}
	}
	for _, change := range []func(*pb.SubmitMessageRequest){func(r *pb.SubmitMessageRequest) { r.Content = "changed" }, func(r *pb.SubmitMessageRequest) { r.Destination.ReceiveId = f.users[2] }} {
		candidate := proto.Clone(request).(*pb.SubmitMessageRequest)
		change(candidate)
		response, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), candidate)
		if status.Code(err) != codes.AlreadyExists || response != nil {
			t.Fatalf("idempotency conflict response=%v err=%v", response, err)
		}
		integrationErrorDetail(t, err, pb.ErrorReason_ERROR_REASON_IDEMPOTENCY_CONFLICT, pb.RetryDirective_RETRY_DIRECTIVE_DO_NOT_RETRY)
	}
	f.count(t, 1)
	if len(f.publisher.records()) != n {
		t.Fatal("conflict published a task")
	}
}

// Replacing routing must Kick the actual old Comet over TCP, and the old
// connection's eventual Disconnect may never delete the second Comet's route.
func TestIntegrationLogicReplacesRouteAndKicksActualOldComet(t *testing.T) {
	f := integrationNew(t)
	old, local := f.connect(t, 0, 0)
	current, newLocal := f.connect(t, 1, 0)
	select {
	case <-local.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Logic did not Kick the old real Comet connection")
	}
	select {
	case <-newLocal.Done():
		t.Fatal("Kick closed replacement connection")
	default:
	}
	response, err := f.client.Disconnect(integrationContext(f.comets[0].token), &pb.DisconnectRequest{Identity: old.Identity})
	if err != nil || response.Outcome != pb.ConditionalDeleteOutcome_CONDITIONAL_DELETE_OUTCOME_NOT_CURRENT {
		t.Fatalf("old disconnect=%v %v", response, err)
	}
	found, err := f.client.LookupRoutes(integrationContext(integrationJobToken), &pb.LookupRoutesRequest{UserIds: []string{f.users[2], f.users[0]}})
	if err != nil || len(found.Results) != 2 || found.Results[0].GetOffline() != pb.OfflineReason_OFFLINE_REASON_NO_ROUTE || !proto.Equal(found.Results[1].GetRoute(), current) {
		t.Fatalf("replacement lost: %v %v", found, err)
	}
	stale := f.request(old, f.users[1], "stale must not store")
	got, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), stale)
	if got != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale submit=%v %v", got, err)
	}
	integrationErrorDetail(t, err, pb.ErrorReason_ERROR_REASON_SUPERSEDED, pb.RetryDirective_RETRY_DIRECTIVE_RECONNECT)
	f.count(t, 0)
	if len(f.publisher.records()) != 0 {
		t.Fatal("stale submit published")
	}
	f.send(t, 1, f.request(current, f.users[1], "current stores"))
	f.count(t, 1)
}

// Membership authorization must be checked before returning a stored replay;
// recipient snapshots must retain offline users even after membership changes.
func TestIntegrationLogicGroupSnapshotAndCurrentAuthorization(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	request := f.request(route, f.group, "one group row")
	first := f.send(t, 0, request)
	replay := f.send(t, 0, request)
	if !replay.Replayed || !proto.Equal(first.Message, replay.Message) || first.Message.SessionId != f.group || first.Message.ConversationId != "group:"+f.group {
		t.Fatalf("group first=%v replay=%v", first, replay)
	}
	f.count(t, 1)
	task := integrationTask(t, f.publisher.records()[0])
	snapshot := integrationSorted(f.users[:]...)
	if !reflect.DeepEqual(task.Recipients, snapshot) || len(task.Targets) != 1 {
		t.Fatalf("offline members lost: %+v", task)
	}
	var persisted struct {
		RecipientIDs       datatypes.JSON
		RequestFingerprint []byte
		ClientMessageID    string
	}
	integrationDB(t, f.db.Table("message").Take(&persisted))
	var recipients []string
	if json.Unmarshal(persisted.RecipientIDs, &recipients) != nil || !reflect.DeepEqual(recipients, snapshot) || len(persisted.RequestFingerprint) != 32 || persisted.ClientMessageID != request.ClientMessageId {
		t.Fatalf("incomplete atomic persistence: %+v", persisted)
	}
	members, _ := json.Marshal([]string{f.users[0], f.users[1]})
	integrationDB(t, f.db.Model(&model.GroupInfo{}).Where("uuid = ?", f.group).Update("members", datatypes.JSON(members)))
	f.send(t, 0, request)
	task = integrationTask(t, f.publisher.records()[2])
	if !reflect.DeepEqual(task.Recipients, snapshot) {
		t.Fatalf("replay refreshed persisted recipient snapshot: %+v", task)
	}
	members, _ = json.Marshal([]string{f.users[1]})
	integrationDB(t, f.db.Model(&model.GroupInfo{}).Where("uuid = ?", f.group).Update("members", datatypes.JSON(members)))
	for _, candidate := range []*pb.SubmitMessageRequest{request, f.request(route, f.group, "new denied")} {
		got, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), candidate)
		if got != nil || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("departed member submit=%v %v", got, err)
		}
	}
	if _, err := f.messages.Sync(context.Background(), f.users[0], "group:"+f.group, "", 10); !errors.Is(err, messagestore.ErrForbidden) {
		t.Fatalf("departed member sync=%v", err)
	}
	f.count(t, 1)
	if len(f.publisher.records()) != 3 {
		t.Fatal("denied membership replay published")
	}
}

// AI normalization and its whitelist apply before idempotent lookup. Ordinary
// traffic retains whitespace and cannot reuse an AI ID to change message origin.
func TestIntegrationLogicAITrimAndRevocation(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	id := uuid.NewString()
	if got, err := f.service.SendAI(context.Background(), f.users[0], f.sessions[0], id, " hello "); got != nil || !errors.Is(err, messagestore.ErrForbidden) {
		t.Fatalf("unlisted AI=%v %v", got, err)
	}
	f.count(t, 0)
	if len(f.publisher.records()) != 0 {
		t.Fatal("unlisted AI published")
	}
	integrationDB(t, f.db.Create(&model.UserAISessionAccess{UserUUID: f.users[0], SessionUUID: f.sessions[0]}))
	first, err := f.service.SendAI(context.Background(), f.users[0], f.sessions[0], id, " \nhello\t ")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.service.SendAI(context.Background(), f.users[0], f.sessions[0], id, "hello")
	if err != nil || first.Message.Content != "hello" || first.Message.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_AI || !replay.Replayed || !proto.Equal(first.Message, replay.Message) {
		t.Fatalf("AI first=%v replay=%v %v", first, replay, err)
	}
	ordinary := f.request(route, f.users[1], "hello")
	ordinary.ClientMessageId = id
	if got, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), ordinary); got != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("origin conflict=%v %v", got, err)
	}
	integrationDB(t, f.db.Where("user_uuid = ?", f.users[0]).Delete(&model.UserAISessionAccess{}))
	for _, candidate := range []string{id, uuid.NewString()} {
		if got, err := f.service.SendAI(context.Background(), f.users[0], f.sessions[0], candidate, "hello"); got != nil || !errors.Is(err, messagestore.ErrForbidden) {
			t.Fatalf("revoked AI=%v %v", got, err)
		}
	}
	f.count(t, 1)
	if len(f.publisher.records()) != 2 {
		t.Fatal("denied AI published")
	}
}

// Caller credentials authorize methods and concrete identity/boot/address grants;
// valid signatures for missing users must still fail the real user table check.
func TestIntegrationLogicAuthorizationHasNoMutation(t *testing.T) {
	f := integrationNew(t)
	c := f.comets[0]
	tests := []struct {
		name, token string
		identity    *pb.CometIdentity
		address     string
		code        codes.Code
	}{
		{"unknown service token", strings.Repeat("x", 32), c.id, c.address, codes.Unauthenticated},
		{"job cannot register", integrationJobToken, c.id, c.address, codes.PermissionDenied},
		{"kick cannot register", integrationKickToken, c.id, c.address, codes.PermissionDenied},
		{"other Comet token", f.comets[1].token, c.id, c.address, codes.PermissionDenied},
		{"old boot", c.token, &pb.CometIdentity{InstanceId: c.id.InstanceId, BootId: uuid.NewString()}, c.address, codes.PermissionDenied},
		{"ungranted address", c.token, c.id, "127.0.0.1:1", codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.client.RegisterComet(integrationContext(tc.token), &pb.RegisterCometRequest{Identity: tc.identity, RpcAddress: tc.address})
			if got != nil || status.Code(err) != tc.code {
				t.Fatalf("registration=%v %v", got, err)
			}
		})
	}
	for _, token := range []string{c.token, integrationKickToken} {
		if got, err := f.client.LookupRoutes(integrationContext(token), &pb.LookupRoutesRequest{UserIds: []string{f.users[0]}}); got != nil || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("non-Job lookup=%v %v", got, err)
		}
		if got, err := f.client.ResolveComets(integrationContext(token), &pb.ResolveCometsRequest{InstanceIds: []string{c.id.InstanceId}}); got != nil || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("non-Job resolve=%v %v", got, err)
		}
	}
	for _, raw := range []string{"invalid.jwt", integrationJWT(t, f.users[0], time.Now().Add(-time.Minute)), integrationJWT(t, "U"+uuid.NewString(), time.Now().Add(time.Hour))} {
		got, err := f.client.Connect(integrationContext(c.token), &pb.ConnectRequest{Comet: c.id, UserJwt: raw, ConnectionId: uuid.NewString(), RouteToken: base64.RawURLEncoding.EncodeToString([]byte(uuid.NewString()))})
		if got != nil || status.Code(err) != codes.Unauthenticated {
			t.Fatalf("invalid user auth=%v %v", got, err)
		}
	}
	routes, err := f.client.LookupRoutes(integrationContext(integrationJobToken), &pb.LookupRoutesRequest{UserIds: f.users[:]})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range routes.Results {
		if r.GetRoute() != nil || r.GetOffline() != pb.OfflineReason_OFFLINE_REASON_NO_ROUTE {
			t.Fatalf("denial changed route: %v", r)
		}
	}
	leases, err := f.client.ResolveComets(integrationContext(integrationJobToken), &pb.ResolveCometsRequest{InstanceIds: []string{c.id.InstanceId, f.comets[1].id.InstanceId}})
	if err != nil {
		t.Fatal(err)
	}
	for n, r := range leases.Results {
		if r.GetLease() == nil || r.GetLease().RpcAddress != f.comets[n].address || !proto.Equal(r.GetLease().Identity, f.comets[n].id) {
			t.Fatalf("denial changed Comet: %v", r)
		}
	}
	f.count(t, 0)
	if len(f.publisher.records()) != 0 {
		t.Fatal("authorization denial published")
	}
}

// Redis renewal must keep the real Instance ready; closing it unregisters its
// lease and closes connections, making still-stored route hints unusable.
func TestIntegrationCometInstanceRenewsAndUnregistersRealLogic(t *testing.T) {
	f := integrationNew(t)
	c := f.comets[0]
	route, local := f.connect(t, 0, 0)
	before, err := f.client.ResolveComets(integrationContext(integrationJobToken), &pb.ResolveCometsRequest{InstanceIds: []string{c.id.InstanceId}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	renewed := false
	for time.Now().Before(deadline) {
		after, err := f.client.ResolveComets(integrationContext(integrationJobToken), &pb.ResolveCometsRequest{InstanceIds: []string{c.id.InstanceId}})
		if err != nil {
			t.Fatal(err)
		}
		if after.Results[0].GetLease().LeaseDeadline.AsTime().After(before.Results[0].GetLease().LeaseDeadline.AsTime()) {
			renewed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !renewed || !c.instance.Ready() {
		t.Fatal("Instance did not renew through real Logic")
	}
	c.instance.Close()
	if c.instance.Ready() {
		t.Fatal("closed Instance is ready")
	}
	select {
	case <-local.Done():
	case <-time.After(time.Second):
		t.Fatal("Instance Close left Hub connection active")
	}
	found, err := f.client.LookupRoutes(integrationContext(integrationJobToken), &pb.LookupRoutesRequest{UserIds: []string{f.users[0]}})
	if err != nil || found.Results[0].GetRoute() != nil {
		t.Fatalf("unregistered Instance route usable=%v %v", found, err)
	}
	if got, err := f.client.SubmitMessage(integrationContext(c.token), f.request(route, f.users[1], "after close")); got != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("closed Instance submit=%v %v", got, err)
	}
	f.count(t, 0)
}

// Once SQL commits, both definite and uncertain broker failures remain normal
// stored results. 64-bit positions survive protobuf, push JSON, and sync cursors.
func TestIntegrationLogicPostCommitDispatchAndLargePositionSync(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	integrationDB(t, f.db.Exec("ALTER TABLE message AUTO_INCREMENT = 9007199254740993"))
	for n, tc := range []struct {
		err   error
		state pb.DispatchState
	}{{logic.ErrNotPublished, pb.DispatchState_DISPATCH_STATE_NOT_QUEUED}, {errors.New("broker acknowledgement unknown"), pb.DispatchState_DISPATCH_STATE_UNKNOWN}, {nil, pb.DispatchState_DISPATCH_STATE_QUEUED}} {
		f.publisher.fail(tc.err)
		result := f.send(t, 0, f.request(route, f.users[1], fmt.Sprintf("message %d", n)))
		if result.Dispatch != tc.state || result.Message.Position != uint64(9007199254740993+n) {
			t.Fatalf("post-commit result=%v", result)
		}
		task := integrationTask(t, f.publisher.records()[n])
		if task.Message.Position != fmt.Sprint(9007199254740993+n) {
			t.Fatalf("push lost position precision: %+v", task)
		}
	}
	f.count(t, 3)
	conversation := "direct:" + f.sessions[0]
	first, err := f.messages.Sync(context.Background(), f.users[1], conversation, "", 2)
	if err != nil || len(first.Messages) != 2 || !first.HasMore || first.Messages[0].Position != "9007199254740993" || first.Messages[1].Position != "9007199254740994" {
		t.Fatalf("first sync=%+v %v", first, err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(first.NextCursor)
	if err != nil || string(decoded) != fmt.Sprintf(`{"v":1,"conversation_id":"%s","after_position":"9007199254740994"}`, conversation) {
		t.Fatalf("cursor=%s %v", decoded, err)
	}
	second, err := f.messages.Sync(context.Background(), f.users[0], conversation, first.NextCursor, 2)
	if err != nil || len(second.Messages) != 1 || second.HasMore || second.Messages[0].Position != "9007199254740995" {
		t.Fatalf("second sync=%+v %v", second, err)
	}
	empty, err := f.messages.Sync(context.Background(), f.users[0], conversation, second.NextCursor, 2)
	if err != nil || empty.Messages == nil || len(empty.Messages) != 0 || empty.NextCursor != second.NextCursor {
		t.Fatalf("empty sync=%+v %v", empty, err)
	}
	if _, err = f.messages.Sync(context.Background(), f.users[2], conversation, "", 2); !errors.Is(err, messagestore.ErrForbidden) {
		t.Fatalf("unauthorized sync=%v", err)
	}
	integrationDB(t, f.db.Where("uuid = ?", f.sessions[0]).Delete(&model.Session{}))
	if _, err = f.messages.Sync(context.Background(), f.users[0], conversation, first.NextCursor, 2); !errors.Is(err, messagestore.ErrForbidden) {
		t.Fatalf("current permission sync=%v", err)
	}
}
func integrationErrorDetail(t *testing.T, err error, reason pb.ErrorReason, retry pb.RetryDirective) {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		if got, ok := detail.(*pb.ErrorDetail); ok {
			if got.Reason != reason || got.Retry != retry {
				t.Fatalf("detail=%v", got)
			}
			return
		}
	}
	t.Fatalf("missing typed ErrorDetail: %v", err)
}

// A caller with another role or Comet grant cannot mutate an existing route or
// persist a sender's message; invalid destination enums also fail before saving.
func TestIntegrationLogicConnectionMethodsEnforceCallerAndValidateDestination(t *testing.T) {
	f := integrationNew(t)
	route, local := f.connect(t, 0, 0)
	request := f.request(route, f.users[1], "must be denied")
	for _, token := range []string{integrationJobToken, integrationKickToken, f.comets[1].token, strings.Repeat("z", 32)} {
		want := codes.PermissionDenied
		if token == strings.Repeat("z", 32) {
			want = codes.Unauthenticated
		}
		if got, err := f.client.SubmitMessage(integrationContext(token), request); got != nil || status.Code(err) != want {
			t.Fatalf("cross-role Submit=%v %v", got, err)
		}
		if got, err := f.client.Heartbeat(integrationContext(token), &pb.HeartbeatRequest{Identity: route.Identity}); got != nil || status.Code(err) != want {
			t.Fatalf("cross-role Heartbeat=%v %v", got, err)
		}
		if got, err := f.client.Disconnect(integrationContext(token), &pb.DisconnectRequest{Identity: route.Identity}); got != nil || status.Code(err) != want {
			t.Fatalf("cross-role Disconnect=%v %v", got, err)
		}
	}
	for _, destination := range []*pb.Destination{nil, {ReceiveType: pb.ReceiveType_RECEIVE_TYPE_UNSPECIFIED, ReceiveId: f.users[1]}, {ReceiveType: pb.ReceiveType(99), ReceiveId: f.users[1]}, {ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: f.group}, {ReceiveType: pb.ReceiveType_RECEIVE_TYPE_GROUP, ReceiveId: f.users[1]}} {
		candidate := proto.Clone(request).(*pb.SubmitMessageRequest)
		candidate.Destination = destination
		if got, err := f.client.SubmitMessage(integrationContext(f.comets[0].token), candidate); got != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid destination=%v result=%v err=%v", destination, got, err)
		}
	}
	select {
	case <-local.Done():
		t.Fatal("denial closed authorized local connection")
	default:
	}
	found, err := f.client.LookupRoutes(integrationContext(integrationJobToken), &pb.LookupRoutesRequest{UserIds: []string{f.users[0]}})
	if err != nil || !proto.Equal(found.Results[0].GetRoute(), route) {
		t.Fatalf("denial mutated authorized route=%v %v", found, err)
	}
	f.count(t, 0)
	if len(f.publisher.records()) != 0 {
		t.Fatal("denied connection method published")
	}
}

// Acknowledging the first chunk and definitely rejecting the next still leaves
// an overall uncertain dispatch, with one durable group message and no lost users.
func TestIntegrationLogicPartialChunkPublishKeepsOneStoredGroupMessage(t *testing.T) {
	f := integrationNew(t)
	route, _ := f.connect(t, 0, 0)
	members := append([]string(nil), f.users[:]...)
	for n := 0; n < 101; n++ {
		id := "U" + uuid.NewString()
		members = append(members, id)
		integrationDB(t, f.db.Create(&model.UserInfo{UUID: id, Nickname: "offline", Telephone: fmt.Sprintf("offline-%d", n), PasswordHash: "test"}))
	}
	raw, _ := json.Marshal(members)
	integrationDB(t, f.db.Model(&model.GroupInfo{}).Where("uuid = ?", f.group).Update("members", datatypes.JSON(raw)))
	f.publisher.mu.Lock()
	f.publisher.err = logic.ErrNotPublished
	f.publisher.failAt = 2
	f.publisher.mu.Unlock()
	result := f.send(t, 0, f.request(route, f.group, "chunked group"))
	if result.Dispatch != pb.DispatchState_DISPATCH_STATE_UNKNOWN {
		t.Fatalf("partial publish pretended fully not queued: %v", result)
	}
	f.count(t, 1)
	records := f.publisher.records()
	if len(records) != 2 {
		t.Fatalf("chunk attempts=%d", len(records))
	}
	var all []string
	for n, record := range records {
		var task struct {
			SchemaVersion uint32   `json:"schema_version"`
			Kind          string   `json:"kind"`
			TaskID        string   `json:"task_id"`
			Recipients    []string `json:"recipients"`
			ChunkIndex    uint32   `json:"chunk_index"`
			ChunkCount    uint32   `json:"chunk_count"`
		}
		// Public DTO decoding also checks production field names below.
		var decoded map[string]json.RawMessage
		if err := json.Unmarshal(record.raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(record.raw, &task); err != nil {
			t.Fatal(err)
		}
		var message struct {
			MessageID string `json:"message_id"`
			Position  string `json:"position"`
		}
		if err := json.Unmarshal(decoded["message"], &message); err != nil {
			t.Fatal(err)
		}
		if task.SchemaVersion != 1 || task.Kind != "message_push" || task.TaskID != fmt.Sprintf("push:v1:%s:%d", result.Message.MessageId, n) || task.ChunkIndex != uint32(n) || task.ChunkCount != 2 || record.key != "group:"+f.group || message.MessageID != result.Message.MessageId || message.Position != fmt.Sprint(result.Message.Position) {
			t.Fatalf("chunk identity=%s", record.raw)
		}
		wantCount := 100
		if n == 1 {
			wantCount = 4
		}
		if len(task.Recipients) != wantCount {
			t.Fatalf("chunk=%d recipients=%d", n, len(task.Recipients))
		}
		all = append(all, task.Recipients...)
	}
	if !reflect.DeepEqual(all, integrationSorted(members...)) {
		t.Fatalf("chunking lost or reordered recipients: %d", len(all))
	}
}

// Repeating the exact Connect tuple may renew the Redis route, but a defensive
// repeat must not Kick the same still-current local connection.
func TestIntegrationLogicExactConnectRepeatDoesNotKickItself(t *testing.T) {
	f := integrationNew(t)
	route, local := f.connect(t, 0, 0)
	c := f.comets[0]
	response, err := f.client.Connect(integrationContext(c.token), &pb.ConnectRequest{Comet: c.id, UserJwt: integrationJWT(t, f.users[0], time.Now().Add(time.Hour)), ConnectionId: route.Identity.ConnectionId, RouteToken: route.Identity.RouteToken})
	if err != nil || response.Route == nil || !proto.Equal(response.Route.Identity, route.Identity) {
		t.Fatalf("exact Connect repeat=%v %v", response, err)
	}
	// A real erroneous asynchronous Kick arrives over localhost TCP. Allow its
	// full RPC budget before asserting that the live connection remains active.
	select {
	case <-local.Done():
		t.Fatal("exact Connect repeat kicked its own still-current Comet connection")
	case <-time.After(2200 * time.Millisecond):
	}
	f.send(t, 0, f.request(response.Route, f.users[1], "same connection remains usable"))
	f.count(t, 1)
}
