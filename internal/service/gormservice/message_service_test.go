package gormservice

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetMessageListReturnsBothDirectionsInOrder(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `message` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY created_at ASC, id ASC")).
		WithArgs("U001", "U002", "U002", "U001").
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "session_id", "type", "content", "send_id", "receive_id", "created_at"}).
			AddRow(1, "M001", "S001", 0, "hello", "U001", "U002", time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)).
			AddRow(2, "M002", "S001", 0, "hi", "U002", "U001", time.Date(2026, 8, 5, 10, 1, 0, 0, time.UTC)))

	result, err := GetMessageList("U001", "U002")
	if err != nil {
		t.Fatalf("GetMessageList() error = %v", err)
	}
	if len(result) != 2 || result[0].UUID != "M001" || result[1].SendID != "U002" {
		t.Fatalf("result = %+v, want both ordered messages", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetMessageListRejectsInvalidPair(t *testing.T) {
	result, err := GetMessageList("U001", "U001")
	if result != nil {
		t.Fatalf("result = %+v, want nil", result)
	}
	if err != ErrInvalidUUID {
		t.Fatalf("error = %v, want %v", err, ErrInvalidUUID)
	}
}
