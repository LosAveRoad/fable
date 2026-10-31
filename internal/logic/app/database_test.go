package app

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"testing"
)

func mockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	return db, m
}
func TestInitRefusesAnyNonemptySchemaWithoutDDL(t *testing.T) {
	db, m := mockDB(t)
	m.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.tables").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if err := initialize(context.Background(), db); err == nil {
		t.Fatal("nonempty database accepted")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestServeSchemaVerificationNeverMigrates(t *testing.T) {
	db, m := mockDB(t)
	m.ExpectQuery("SELECT COUNT\\(DISTINCT table_name\\)").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(6))
	if err := verifySchema(context.Background(), db); err == nil {
		t.Fatal("missing target table accepted")
	}
	if err := m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestServeSchemaVerificationRequiresNonNullColumnsAndUniqueIndex(t *testing.T) {
	for _, tc := range []struct{ columns, index int }{{2, 0}, {3, 0}, {3, 1}} {
		db, m := mockDB(t)
		m.ExpectQuery("SELECT COUNT\\(DISTINCT table_name\\)").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(7))
		m.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.columns))
		if tc.columns == 3 {
			m.ExpectQuery("SELECT COUNT\\(\\*\\) FROM \\(SELECT index_name").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tc.index))
		}
		err := verifySchema(context.Background(), db)
		if (err == nil) != (tc.columns == 3 && tc.index == 1) {
			t.Fatalf("columns=%d index=%d err=%v", tc.columns, tc.index, err)
		}
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}
