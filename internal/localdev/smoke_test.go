package localdev

import (
	"context"
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	logicapp "mychat/internal/logic/app"
	"regexp"
	"testing"
)

func TestSelectedSmokeRefusesForeignDatabaseAndNonlocalIngress(t *testing.T) {
	for _, tc := range []struct{ database, entry string }{
		{"production", "http://127.0.0.1:18080"},
		{"fable_local_test", "http://example.com:18080"},
		{"fable_local_test", "http://127.0.0.1:18080/unrelated"},
		{"fable_local_test", "http://secret@127.0.0.1:18080"},
		{"fable_local_test", "http://127.0.0.1:18080?token=secret"},
	} {
		c := logicapp.Config{}
		c.Base.MySQLConfig.DatabaseName = tc.database
		if err := Smoke(context.Background(), c, tc.entry); err == nil {
			t.Fatalf("accepted unsafe smoke target %q %q", tc.database, tc.entry)
		}
	}
}

func TestSmokeCleanupDeletesOnlyExactFixtureIDs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	users := [2]string{"U-own-b", "U-own-a"}
	ids := [2]string{"client-b", "client-a"}
	mock.ExpectBegin()
	for _, user := range []string{users[1], users[0]} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT uuid FROM user_info WHERE uuid=? FOR UPDATE")).WithArgs(user).WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(user))
	}
	for i := range users {
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM message WHERE session_id=? AND send_id=? AND receive_id=? AND client_message_id=?")).WithArgs("S-own", users[i], users[1-i], ids[i]).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM session WHERE uuid=? AND send_id=? AND receive_id=?")).WithArgs("S-own", users[1], users[0]).WillReturnResult(sqlmock.NewResult(0, 1))
	for _, user := range users {
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM user_info WHERE uuid=?")).WithArgs(user).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectCommit()
	if err = cleanupSmoke(context.Background(), db, users, ids, "S-own"); err != nil {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSmokeCleanupFailureRollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	for _, user := range []string{"U-a", "U-b"} {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT uuid FROM user_info WHERE uuid=? FOR UPDATE")).WithArgs(user).WillReturnRows(sqlmock.NewRows([]string{"uuid"}).AddRow(user))
	}
	mock.ExpectExec("DELETE FROM message WHERE session_id=").WithArgs("S-own", "U-a", "U-b", "id-a").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()
	if err = cleanupSmoke(context.Background(), db, [2]string{"U-a", "U-b"}, [2]string{"id-a", "id-b"}, "S-own"); err == nil {
		t.Fatal("ignored cleanup failure")
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
