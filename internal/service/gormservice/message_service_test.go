package gormservice

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"mychat/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSendMessagePersistsSessionMessage(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	createdAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY `session`.`id` LIMIT ?")).
		WithArgs("U001", "U002", "U002", "U001", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}).
			AddRow(1, "S001", "U001", "U002", createdAt))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `message`").
		WithArgs(sqlmock.AnyArg(), "S001", 0, "hello", 0, "U001", "U002", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := SendMessage("U001", "U002", "hello")
	if err != nil {
		t.Fatalf("SendMessage() error = %v", err)
	}
	if result.UUID == "" || result.SessionID != "S001" || result.SendID != "U001" || result.ReceiveID != "U002" || result.Content != "hello" || result.Origin != model.MessageOriginUser {
		t.Fatalf("result = %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSendAIMessagePersistsAIOrigin(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	createdAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY `session`.`id` LIMIT ?")).
		WithArgs("U001", "U002", "U002", "U001", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}).
			AddRow(1, "S001", "U001", "U002", createdAt))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO `message`").
		WithArgs(sqlmock.AnyArg(), "S001", 0, "hello from AI", model.MessageOriginAI, "U001", "U002", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := SendAIMessage("U001", "U002", "hello from AI")
	if err != nil {
		t.Fatalf("SendAIMessage() error = %v", err)
	}
	if result.Origin != model.MessageOriginAI {
		t.Fatalf("origin = %d, want %d", result.Origin, model.MessageOriginAI)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSendMessageRejectsMissingSession(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY `session`.`id` LIMIT ?")).
		WithArgs("U001", "U002", "U002", "U001", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}))

	_, err := SendMessage("U001", "U002", "hello")
	if !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidSession)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSendMessageRejectsInvalidInput(t *testing.T) {
	_, err := SendMessage("U001", "U001", "hello")
	if !errors.Is(err, ErrInvalidUserPair) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidUserPair)
	}

	_, err = SendMessage("U001", "U002", "")
	if !errors.Is(err, ErrInvalidMessageContent) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidMessageContent)
	}
}

func TestGetMessageListReturnsBothDirectionsInOrder(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `message` WHERE (send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?) ORDER BY created_at ASC, id ASC")).
		WithArgs("U001", "U002", "U002", "U001").
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "session_id", "type", "content", "origin", "send_id", "receive_id", "created_at"}).
			AddRow(1, "M001", "S001", 0, "hello", model.MessageOriginAI, "U001", "U002", time.Date(2026, 8, 5, 10, 0, 0, 0, time.UTC)).
			AddRow(2, "M002", "S001", 0, "hi", model.MessageOriginUser, "U002", "U001", time.Date(2026, 8, 5, 10, 1, 0, 0, time.UTC)))

	result, err := GetMessageList("U001", "U002")
	if err != nil {
		t.Fatalf("GetMessageList() error = %v", err)
	}
	if len(result) != 2 || result[0].UUID != "M001" || result[0].Origin != model.MessageOriginAI || result[1].SendID != "U002" {
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
