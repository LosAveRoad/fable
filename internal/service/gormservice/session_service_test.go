package gormservice

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOpenSessionReusesExistingSession(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `user_info` WHERE uuid = ?")).
		WithArgs("U001").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE send_id = ? AND receive_id = ? ORDER BY `session`.`id` LIMIT ?")).
		WithArgs("U001", "U002", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}).
			AddRow(1, "S001", "U001", "U002", time.Now()))

	result, err := OpenSession("U002", "U001")
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	if result.SessionUUID != "S001" {
		t.Fatalf("session uuid = %q, want S001", result.SessionUUID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetUserSessionListReturnsPeerUUID(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE send_id = ? OR receive_id = ? ORDER BY created_at ASC, id ASC")).
		WithArgs("U001", "U001").
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}).
			AddRow(1, "S001", "U001", "U002", time.Now()).
			AddRow(2, "S002", "U003", "U001", time.Now()))

	result, err := GetUserSessionList("U001")
	if err != nil {
		t.Fatalf("GetUserSessionList() error = %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("result length = %d, want 2", len(result))
	}
	if result[0].SessionUUID != "S001" || result[0].PeerUUID != "U002" {
		t.Fatalf("first session = %+v, want S001/U002", result[0])
	}
	if result[1].SessionUUID != "S002" || result[1].PeerUUID != "U003" {
		t.Fatalf("second session = %+v, want S002/U003", result[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenSessionRejectsSelfChat(t *testing.T) {
	result, err := OpenSession("U001", "U001")
	if result.SessionUUID != "" {
		t.Fatalf("session uuid = %q, want empty", result.SessionUUID)
	}
	if err != ErrInvalidSession {
		t.Fatalf("error = %v, want %v", err, ErrInvalidSession)
	}
}
