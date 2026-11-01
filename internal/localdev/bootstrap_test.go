package localdev

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

// A broad CREATE/DROP or schema init on a populated DB would break this test.
func TestEnsureDatabaseRejectsForeignNamesAndPreservesPopulatedTables(t *testing.T) {
	db, mock, e := sqlmock.New()
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for _, name := range []string{"production", "mysql", "fable_local_`unsafe", ""} {
		if _, e := ensureDatabase(context.Background(), db, name); e == nil {
			t.Fatalf("accepted foreign name %q", name)
		}
	}
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `fable_local_example`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COUNT").WithArgs("fable_local_example").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))
	empty, e := ensureDatabase(context.Background(), db, "fable_local_example")
	if e != nil || empty {
		t.Fatalf("nonempty target treated as new: %v %v", empty, e)
	}
	if e := mock.ExpectationsWereMet(); e != nil {
		t.Fatal(e)
	}
}
