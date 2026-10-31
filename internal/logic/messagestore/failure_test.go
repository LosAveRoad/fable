package messagestore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func mockedStore(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	raw, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: raw, SkipInitializeWithVersion: true}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	return New(db), mock
}
func expectDirectAuthorization(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT .* FROM `user_info` WHERE uuid = .* FOR UPDATE").WithArgs(userA, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "uuid"}).AddRow(1, userA))
	mock.ExpectQuery("SELECT .* FROM `session` WHERE type = .* FOR UPDATE").WithArgs(0, userA, userB, userB, userA, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "type"}).AddRow(1, sessionAB, userA, userB, 0))
	mock.ExpectQuery("SELECT count\\(\\*\\) FROM `user_info` WHERE uuid IN ").WithArgs(userA, userB).WillReturnRows(sqlmock.NewRows([]string{"count(*)"}).AddRow(2))
}
func expectNoDuplicate(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT .* FROM `message` WHERE send_id = .* FOR UPDATE").WithArgs(userA, clientID, 1).WillReturnRows(sqlmock.NewRows([]string{"id"}))
}

// A failed commit must not return a message that the caller can publish.
func TestCommitFailureDiscardsSavedResult(t *testing.T) {
	s, mock := mockedStore(t)
	mock.ExpectBegin()
	expectDirectAuthorization(mock)
	expectNoDuplicate(mock)
	mock.ExpectExec("INSERT INTO `message`").WillReturnResult(sqlmock.NewResult(42, 1))
	mock.ExpectCommit().WillReturnError(errors.New("private commit details"))
	got, err := s.Save(context.Background(), validInput())
	if !errors.Is(err, ErrDatabase) || got.Message != nil || got.Recipients != nil {
		t.Fatalf("Save=%+v %v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizationDatabaseFailureRollsBack(t *testing.T) {
	s, mock := mockedStore(t)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM `user_info`").WithArgs(userA, 1).WillReturnError(errors.New("private auth query details"))
	mock.ExpectRollback()
	got, err := s.Save(context.Background(), validInput())
	if !errors.Is(err, ErrDatabase) || got.Message != nil || err.Error() != "message database unavailable" {
		t.Fatalf("Save=%+v %v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertFailureRollsBack(t *testing.T) {
	s, mock := mockedStore(t)
	mock.ExpectBegin()
	expectDirectAuthorization(mock)
	expectNoDuplicate(mock)
	mock.ExpectExec("INSERT INTO `message`").WillReturnError(errors.New("private insert details"))
	mock.ExpectRollback()
	got, err := s.Save(context.Background(), validInput())
	if !errors.Is(err, ErrDatabase) || got.Message != nil {
		t.Fatalf("Save=%+v %v", got, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSyncDatabaseFailureDiscardsPartialPage(t *testing.T) {
	s, mock := mockedStore(t)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM `user_info`").WithArgs(userA, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "uuid"}).AddRow(1, userA))
	mock.ExpectQuery("SELECT .* FROM `session`").WithArgs(0, sessionAB, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "type"}).AddRow(1, sessionAB, userA, userB, 0))
	mock.ExpectQuery("SELECT .* FROM `message`").WithArgs(sessionAB, 0, 3).WillReturnError(errors.New("private query details"))
	mock.ExpectRollback()
	page, err := s.Sync(context.Background(), userA, "direct:"+sessionAB, "", 2)
	if !errors.Is(err, ErrDatabase) || len(page.Messages) != 0 || page.NextCursor != "" {
		t.Fatalf("Sync=%+v %v", page, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledContextDoesNotReturnSavedResult(t *testing.T) {
	s, _ := mockedStore(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got, err := s.Save(ctx, validInput())
	if !errors.Is(err, ErrDatabase) || got.Message != nil {
		t.Fatalf("Save=%+v %v", got, err)
	}
}
