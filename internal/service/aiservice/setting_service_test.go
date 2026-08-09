package aiservice

import (
	"regexp"
	"testing"
	"time"

	"mychat/internal/dao"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func newMockGormDB(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatal(err)
	}
	oldDB := dao.GormDB
	dao.GormDB = db
	return mock, func() {
		dao.GormDB = oldDB
		_ = sqlDB.Close()
	}
}

func TestNormalizeSessionUUIDs(t *testing.T) {
	got, err := normalizeSessionUUIDs([]string{" S002 ", "S001", "S002"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "S001" || got[1] != "S002" {
		t.Fatalf("normalizeSessionUUIDs() = %#v", got)
	}

	if _, err := normalizeSessionUUIDs([]string{""}); err != ErrInvalidSetting {
		t.Fatalf("empty ID error = %v, want %v", err, ErrInvalidSetting)
	}
}

func TestGetAISettingCombinesSessionsAndAccess(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()
	now := time.Now()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `session` WHERE send_id = ? OR receive_id = ? ORDER BY created_at ASC, id ASC")).
		WithArgs("U001", "U001").
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "send_id", "receive_id", "created_at"}).
			AddRow(1, "S001", "U001", "U002", now).
			AddRow(2, "S002", "U003", "U001", now))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `user_ai_session_access` WHERE user_uuid = ? AND session_uuid IN (?,?)")).
		WithArgs("U001", "S001", "S002").
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_uuid", "session_uuid", "created_at", "updated_at"}).
			AddRow(1, "U001", "S002", now, now))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `user_info` WHERE uuid IN (?,?)")).
		WithArgs("U002", "U003").
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "nickname", "telephone", "password_hash", "created_at"}).
			AddRow(2, "U002", "Alice", "13800000002", "hash", now).
			AddRow(3, "U003", "Bob", "13800000003", "hash", now))

	setting, err := GetAISetting("U001")
	if err != nil {
		t.Fatal(err)
	}
	if len(setting.Sessions) != 2 {
		t.Fatalf("sessions = %d, want 2", len(setting.Sessions))
	}
	if setting.Sessions[0].Peer.Name != "Alice" || setting.Sessions[0].AIAccessAllowed {
		t.Fatalf("first session = %+v", setting.Sessions[0])
	}
	if setting.Sessions[1].Peer.Name != "Bob" || !setting.Sessions[1].AIAccessAllowed {
		t.Fatalf("second session = %+v", setting.Sessions[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestChangeAISettingRollsBackForUnauthorizedSession(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `session` WHERE uuid IN (?) AND (send_id = ? OR receive_id = ?)")).
		WithArgs("S999", "U001", "U001").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectRollback()

	_, err := ChangeAISetting("U001", []string{"S999"})
	if err != ErrForbidden {
		t.Fatalf("error = %v, want %v", err, ErrForbidden)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
