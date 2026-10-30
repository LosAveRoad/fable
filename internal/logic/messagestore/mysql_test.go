//go:build mysql

package messagestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/model"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func mysqlFixture(t *testing.T) (*gorm.DB, *Store) {
	t.Helper()
	dsn := os.Getenv("FABLE_LOGIC_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("FABLE_LOGIC_TEST_MYSQL_DSN is required for real MySQL tests")
	}
	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ParseTime = true
	cfg.DBName = ""
	admin, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	name := "fable_logic_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := admin.Exec("CREATE DATABASE `" + name + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_bin").Error; err != nil {
		t.Fatal(err)
	}
	cfg.DBName = name
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(20)
	t.Cleanup(func() { sqlDB.Close(); admin.Exec("DROP DATABASE `" + name + "`"); a, _ := admin.DB(); a.Close() })
	if err := InitSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{userA, userB, userC} {
		if err := db.Create(&model.UserInfo{UUID: id, Nickname: fmt.Sprint(i), Telephone: fmt.Sprint(i), PasswordHash: "test"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range []model.Session{{UUID: sessionAB, SendId: userA, ReceiveId: userB}, {UUID: sessionAC, SendId: userA, ReceiveId: userC}} {
		if err := db.Create(&s).Error; err != nil {
			t.Fatal(err)
		}
	}
	return db, New(db)
}
func mustSave(t *testing.T, s *Store, in Input) Saved {
	t.Helper()
	v, err := s.Save(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func newClient() string { return uuid.NewString() }
func makeGroup(t *testing.T, db *gorm.DB, members []string) {
	t.Helper()
	b, _ := json.Marshal(members)
	g := model.GroupInfo{UUID: groupID, Name: "group", OwnerID: userA, Members: datatypes.JSON(b), Admins: datatypes.JSON(`[]`), MemberCount: len(members)}
	if err := db.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
}
func groupInput() Input {
	in := validInput()
	in.Destination = &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_GROUP, ReceiveId: groupID}
	return in
}
func assertCount(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	var got int64
	if err := db.Table("message").Count(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("message count=%d, want %d", got, want)
	}
}

// A missing unique key/serialization would produce two rows or different results.
func TestMySQLConcurrentDuplicateAndConflict(t *testing.T) {
	db, s := mysqlFixture(t)
	in := validInput()
	const n = 12
	results := make(chan Saved, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; v, e := s.Save(context.Background(), in); results <- v; errs <- e }()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	id := ""
	fresh := 0
	for v := range results {
		if id == "" {
			id = v.Message.MessageId
		}
		if v.Message.MessageId != id || v.Message.Position == 0 {
			t.Fatalf("unstable result: %v", v)
		}
		if !v.Replayed {
			fresh++
		}
		if strings.Join(v.Recipients, ",") != userA+","+userB {
			t.Fatalf("recipients=%v", v.Recipients)
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh=%d", fresh)
	}
	assertCount(t, db, 1)
	for _, change := range []func(*Input){func(in *Input) { in.Content = "changed" }, func(in *Input) { in.Destination.ReceiveId = userC }, func(in *Input) {
		in.Destination = &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_GROUP, ReceiveId: groupID}
	}} {
		makeGroupIfNeeded := false
		candidate := in
		candidate.Destination = &pb.Destination{ReceiveType: in.Destination.ReceiveType, ReceiveId: in.Destination.ReceiveId}
		change(&candidate)
		makeGroupIfNeeded = candidate.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP
		if makeGroupIfNeeded {
			makeGroup(t, db, []string{userA, userB})
		}
		if _, e := s.Save(context.Background(), candidate); !errors.Is(e, ErrConflict) {
			t.Fatalf("changed payload error=%v", e)
		}
	}
	assertCount(t, db, 1)
}

func TestMySQLSnapshotAndWhitespace(t *testing.T) {
	db, s := mysqlFixture(t)
	makeGroup(t, db, []string{userC, userA, userB, userA})
	in := groupInput()
	in.Content = " \n hello\t "
	first := mustSave(t, s, in)
	if first.Message.Content != in.Content || first.Message.ConversationId != "group:"+groupID || first.Message.SessionId != groupID || first.Message.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_USER {
		t.Fatalf("message=%v", first.Message)
	}
	if strings.Join(first.Recipients, ",") != userA+","+userB+","+userC {
		t.Fatalf("recipients=%v", first.Recipients)
	}
	if err := db.Model(&model.GroupInfo{}).Where("uuid = ?", groupID).Update("members", datatypes.JSON(`["`+userA+`","`+userB+`"]`)).Error; err != nil {
		t.Fatal(err)
	}
	replay := mustSave(t, s, in)
	if !replay.Replayed || strings.Join(replay.Recipients, ",") != strings.Join(first.Recipients, ",") {
		t.Fatalf("snapshot changed: %v", replay)
	}
	var raw struct {
		ClientMessageID    string
		RequestFingerprint []byte
		RecipientIDs       datatypes.JSON
	}
	if err := db.Table("message").Take(&raw).Error; err != nil {
		t.Fatal(err)
	}
	if raw.ClientMessageID != clientID || len(raw.RequestFingerprint) != 32 || string(raw.RecipientIDs) == "null" {
		t.Fatalf("missing atomic fields: %+v", raw)
	}
	assertCount(t, db, 1)
}

func TestMySQLReauthorizesBeforeReplay(t *testing.T) {
	db, s := mysqlFixture(t)
	makeGroup(t, db, []string{userA, userB})
	in := groupInput()
	mustSave(t, s, in)
	if err := db.Model(&model.GroupInfo{}).Where("uuid = ?", groupID).Update("members", datatypes.JSON(`["`+userB+`"]`)).Error; err != nil {
		t.Fatal(err)
	}
	if v, e := s.Save(context.Background(), in); !errors.Is(e, ErrForbidden) || v.Message != nil {
		t.Fatalf("departed replay=%v %v", v, e)
	}
	if _, e := s.Sync(context.Background(), userA, "group:"+groupID, "", 10); !errors.Is(e, ErrForbidden) {
		t.Fatalf("departed sync error=%v", e)
	}
	if err := db.Model(&model.GroupInfo{}).Where("uuid = ?", groupID).Updates(map[string]any{"members": datatypes.JSON(`["` + userA + `","` + userB + `"]`), "status": model.GroupStatusDismissed}).Error; err != nil {
		t.Fatal(err)
	}
	if _, e := s.Save(context.Background(), in); !errors.Is(e, ErrForbidden) {
		t.Fatalf("dismissed error=%v", e)
	}
	assertCount(t, db, 1)
}

func TestMySQLAIWhitelistOriginAndNormalization(t *testing.T) {
	db, s := mysqlFixture(t)
	ctx := context.Background()
	if _, e := s.SaveAI(ctx, userA, sessionAB, clientID, " hi "); !errors.Is(e, ErrForbidden) {
		t.Fatalf("unlisted=%v", e)
	}
	if err := db.Create(&model.UserAISessionAccess{UserUUID: userA, SessionUUID: sessionAB}).Error; err != nil {
		t.Fatal(err)
	}
	ai, e := s.SaveAI(ctx, userA, sessionAB, clientID, " \nhi\t ")
	if e != nil {
		t.Fatal(e)
	}
	if ai.Message.Content != "hi" || ai.Message.Origin != pb.MessageOrigin_MESSAGE_ORIGIN_AI || ai.Message.Destination.ReceiveId != userB {
		t.Fatalf("AI=%v", ai.Message)
	}
	replay, e := s.SaveAI(ctx, userA, sessionAB, clientID, "hi")
	if e != nil || !replay.Replayed || replay.Message.MessageId != ai.Message.MessageId {
		t.Fatalf("replay=%v %v", replay, e)
	}
	in := validInput()
	in.Content = "hi"
	if _, e := s.Save(ctx, in); !errors.Is(e, ErrConflict) {
		t.Fatalf("origin conflict=%v", e)
	}
	if err := db.Where("user_uuid = ?", userA).Delete(&model.UserAISessionAccess{}).Error; err != nil {
		t.Fatal(err)
	}
	if v, e := s.SaveAI(ctx, userA, sessionAB, clientID, "hi"); !errors.Is(e, ErrForbidden) || v.Message != nil {
		t.Fatalf("revoked=%v %v", v, e)
	}
	page, e := s.Sync(ctx, userA, "direct:"+sessionAB, "", 10)
	if e != nil || len(page.Messages) != 1 {
		t.Fatalf("user sync after AI revoke=%v %v", page, e)
	}
	assertCount(t, db, 1)
}

// Deleting membership while send waits for the group lock must be observed.
func TestMySQLGroupLeaveLockRace(t *testing.T) {
	db, s := mysqlFixture(t)
	makeGroup(t, db, []string{userA, userB})
	ctx := context.Background()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Exec("SELECT id FROM group_info WHERE uuid = ? FOR UPDATE", groupID).Error; err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, e := s.Save(ctx, groupInput()); result <- e }()
	select {
	case e := <-result:
		t.Fatalf("send bypassed held group lock: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Model(&model.GroupInfo{}).Where("uuid = ?", groupID).Update("members", datatypes.JSON(`["`+userB+`"]`)).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-result:
		if !errors.Is(e, ErrForbidden) {
			t.Fatalf("race result=%v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send blocked")
	}
	assertCount(t, db, 0)
}

func TestMySQLAIRevokeLockRace(t *testing.T) {
	db, s := mysqlFixture(t)
	if err := db.Create(&model.UserAISessionAccess{UserUUID: userA, SessionUUID: sessionAB}).Error; err != nil {
		t.Fatal(err)
	}
	tx := db.Begin()
	defer tx.Rollback()
	if err := tx.Where("user_uuid = ?", userA).Delete(&model.UserAISessionAccess{}).Error; err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, e := s.SaveAI(context.Background(), userA, sessionAB, clientID, "hi"); result <- e }()
	select {
	case e := <-result:
		t.Fatalf("send bypassed revoke lock: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-result:
		if !errors.Is(e, ErrForbidden) {
			t.Fatalf("race result=%v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send blocked")
	}
	assertCount(t, db, 0)
}

func TestMySQLRollbackAndMissingAuthority(t *testing.T) {
	db, s := mysqlFixture(t)
	// A server-side error after the insert statement has begun must leave no row.
	if err := db.Exec("CREATE TRIGGER reject_message BEFORE INSERT ON message FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test rollback'").Error; err != nil {
		t.Fatal(err)
	}
	if v, e := s.Save(context.Background(), validInput()); !errors.Is(e, ErrDatabase) || v.Message != nil {
		t.Fatalf("failed insert=%v %v", v, e)
	}
	assertCount(t, db, 0)
	if err := db.Exec("DROP TRIGGER reject_message").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("uuid = ?", userB).Delete(&model.UserInfo{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, e := s.Save(context.Background(), validInput()); !errors.Is(e, ErrForbidden) {
		t.Fatalf("missing peer=%v", e)
	}
	assertCount(t, db, 0)
}

func TestMySQLSyncPositionsCursorAndCurrentPermission(t *testing.T) {
	db, s := mysqlFixture(t)
	ctx := context.Background()
	conversation := "direct:" + sessionAB
	empty, e := s.Sync(ctx, userA, conversation, "", 0)
	if e != nil || empty.Messages == nil || len(empty.Messages) != 0 || empty.NextCursor != "" || empty.HasMore {
		t.Fatalf("empty=%+v %v", empty, e)
	}
	var positions []uint64
	for range 3 {
		in := validInput()
		in.ClientMessageID = newClient()
		v := mustSave(t, s, in)
		positions = append(positions, v.Message.Position)
	}
	first, e := s.Sync(ctx, userB, conversation, "", 2)
	if e != nil || len(first.Messages) != 2 || !first.HasMore {
		t.Fatalf("first=%+v %v", first, e)
	}
	if first.Messages[0].Position != fmt.Sprint(positions[0]) || first.Messages[1].Position != fmt.Sprint(positions[1]) {
		t.Fatalf("positions=%+v", first.Messages)
	}
	decoded, e := base64.RawURLEncoding.DecodeString(first.NextCursor)
	if e != nil || string(decoded) != fmt.Sprintf(`{"v":1,"conversation_id":"%s","after_position":"%d"}`, conversation, positions[1]) {
		t.Fatalf("cursor=%s %v", decoded, e)
	}
	second, e := s.Sync(ctx, userA, conversation, first.NextCursor, 2)
	if e != nil || len(second.Messages) != 1 || second.HasMore || second.Messages[0].Position != fmt.Sprint(positions[2]) {
		t.Fatalf("second=%+v %v", second, e)
	}
	final, e := s.Sync(ctx, userA, conversation, second.NextCursor, 2)
	if e != nil || final.Messages == nil || len(final.Messages) != 0 || final.NextCursor != second.NextCursor {
		t.Fatalf("final=%+v %v", final, e)
	}
	if _, e := s.Sync(ctx, userC, conversation, "", 1); !errors.Is(e, ErrForbidden) {
		t.Fatalf("foreign=%v", e)
	}
	if err := db.Where("uuid = ?", sessionAB).Delete(&model.Session{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, e := s.Sync(ctx, userA, conversation, second.NextCursor, 2); !errors.Is(e, ErrForbidden) {
		t.Fatalf("removed session=%v", e)
	}
}

// A later writer must wait before allocating its ID, so sync never skips an
// earlier uncommitted message while observing a later committed message.
func TestMySQLConversationLockCoversAllocationToCommit(t *testing.T) {
	db, s := mysqlFixture(t)
	tx := db.Begin()
	defer tx.Rollback()
	if err := tx.Exec("SELECT id FROM session WHERE uuid = ? FOR UPDATE", sessionAB).Error; err != nil {
		t.Fatal(err)
	}
	in := validInput()
	in.SenderID = userB
	in.Destination.ReceiveId = userA
	done := make(chan Saved, 1)
	errs := make(chan error, 1)
	go func() { v, e := s.Save(context.Background(), in); done <- v; errs <- e }()
	select {
	case e := <-errs:
		t.Fatalf("writer bypassed conversation lock: %v", e)
	case <-time.After(100 * time.Millisecond):
	}
	// Insert a committed predecessor under the very same conversation lock.
	if err := tx.Exec("INSERT INTO message (uuid,session_id,type,content,origin,send_id,receive_id,created_at,client_message_id,request_fingerprint,recipient_ids) VALUES (?,?,0,'first',0,?,?,UTC_TIMESTAMP(6),?,UNHEX(SHA2('first',256)),?)", "M"+newClient(), sessionAB, userA, userB, newClient(), `["`+userA+`","`+userB+`"]`).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-errs:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer stuck")
	}
	v := <-done
	page, e := s.Sync(context.Background(), userA, "direct:"+sessionAB, "", 10)
	if e != nil || len(page.Messages) != 2 || page.Messages[0].Content != "first" || page.Messages[1].Position != fmt.Sprint(v.Message.Position) {
		t.Fatalf("page=%+v %v", page, e)
	}
}

func TestMySQLLargeRecipientSnapshot(t *testing.T) {
	db, s := mysqlFixture(t)
	members := []string{userA, userB, userC}
	for i := 0; i < 105; i++ {
		id := "U" + newClient()
		members = append(members, id)
		if err := db.Create(&model.UserInfo{UUID: id, Nickname: "member", Telephone: fmt.Sprintf("phone%d", i), PasswordHash: "test"}).Error; err != nil {
			t.Fatal(err)
		}
	}
	makeGroup(t, db, members)
	saved := mustSave(t, s, groupInput())
	sort.Strings(members)
	if strings.Join(saved.Recipients, ",") != strings.Join(members, ",") {
		t.Fatalf("snapshot truncated %d", len(saved.Recipients))
	}
}

// Replays must return byte-for-byte equivalent persisted timestamps, not the
// original process's higher precision clock value.
func TestMySQLReplayReturnsIdenticalMessage(t *testing.T) {
	_, s := mysqlFixture(t)
	in := validInput()
	first := mustSave(t, s, in)
	replay := mustSave(t, s, in)
	if !proto.Equal(first.Message, replay.Message) {
		t.Fatalf("saved and replay differ: first=%v replay=%v", first.Message, replay.Message)
	}
}

func TestMySQLConcurrentDifferentDestinationsConflict(t *testing.T) {
	db, s := mysqlFixture(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, peer := range []string{userB, userC} {
		go func(peer string) {
			<-start
			in := validInput()
			in.Destination.ReceiveId = peer
			_, e := s.Save(context.Background(), in)
			results <- e
		}(peer)
	}
	close(start)
	success, conflict := 0, 0
	for range 2 {
		e := <-results
		if e == nil {
			success++
		} else if errors.Is(e, ErrConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	assertCount(t, db, 1)
}

func TestMySQLSyncLargePositionsAndDefaultLimit(t *testing.T) {
	db, s := mysqlFixture(t)
	ctx := context.Background()
	if err := db.Exec("ALTER TABLE message AUTO_INCREMENT = 9007199254740993").Error; err != nil {
		t.Fatal(err)
	}
	for range 103 {
		in := validInput()
		in.ClientMessageID = newClient()
		mustSave(t, s, in)
	}
	// A message in another conversation must never enter this page.
	in := validInput()
	in.ClientMessageID = newClient()
	in.Destination.ReceiveId = userC
	mustSave(t, s, in)
	first, e := s.Sync(ctx, userA, "direct:"+sessionAB, "", 0)
	if e != nil || len(first.Messages) != 100 || !first.HasMore || first.Messages[0].Position != "9007199254740993" || first.Messages[99].Position != "9007199254741092" {
		t.Fatalf("default page count=%d more=%v error=%v", len(first.Messages), first.HasMore, e)
	}
	rest, e := s.Sync(ctx, userB, "direct:"+sessionAB, first.NextCursor, 500)
	if e != nil || len(rest.Messages) != 3 || rest.HasMore || rest.Messages[0].Position != "9007199254741093" {
		t.Fatalf("rest=%+v %v", rest, e)
	}
}

func TestMySQLFingerprintV1StableEncoding(t *testing.T) {
	db, s := mysqlFixture(t)
	mustSave(t, s, validInput())
	var raw struct{ RequestFingerprint []byte }
	if e := db.Table("message").Select("request_fingerprint").Take(&raw).Error; e != nil {
		t.Fatal(e)
	}
	// This literal is the encoding contract, independent of the production struct.
	want := sha256.Sum256([]byte(`{"v":1,"destination":{"receive_type":"user","receive_id":"U10000000-0000-4000-8000-000000000002"},"content":"hello","type":"text","origin":"user"}`))
	if !bytes.Equal(raw.RequestFingerprint, want[:]) {
		t.Fatalf("fingerprint=%x want=%x", raw.RequestFingerprint, want)
	}
}

// MySQL DATETIME(6) cannot retain nanoseconds. Using the configured database
// clock with explicit microsecond precision keeps fresh and replayed views equal
// on platforms whose clocks have nanosecond resolution.
func TestMySQLConfiguredClockPreservesReplayTimestamp(t *testing.T) {
	db, s := mysqlFixture(t)
	db.Config.NowFunc = func() time.Time { return time.Date(2026, 10, 30, 12, 34, 56, 123456789, time.UTC) }
	first := mustSave(t, s, validInput())
	replay := mustSave(t, s, validInput())
	want := time.Date(2026, 10, 30, 12, 34, 56, 123456000, time.UTC)
	if !first.Message.CreatedAt.AsTime().Equal(want) || !proto.Equal(first.Message, replay.Message) {
		t.Fatalf("first=%v replay=%v want=%v", first.Message.CreatedAt, replay.Message.CreatedAt, want)
	}
}

func TestMySQLInitSchemaIncludesContactBusinessTables(t *testing.T) {
	db, _ := mysqlFixture(t)
	contact := model.UserContact{UserID: userA, ContactID: userB, ContactType: model.ContactTypeUser}
	if err := db.Create(&contact).Error; err != nil {
		t.Fatalf("contact service table unavailable: %v", err)
	}
	apply := model.ContactApply{ApplicantID: userA, ContactID: groupID, ContactType: model.ContactTypeGroup}
	if err := db.Create(&apply).Error; err != nil {
		t.Fatalf("group approval table unavailable: %v", err)
	}
	if contact.ID <= 0 || apply.ID <= 0 {
		t.Fatalf("business rows not persisted: contact=%+v apply=%+v", contact, apply)
	}
}
