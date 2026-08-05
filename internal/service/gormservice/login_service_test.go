package gormservice

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"mychat/internal/config"
	"mychat/internal/dao"
	"mychat/internal/dto/request"
)

func newMockGormDB(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("create gorm db: %v", err)
	}

	oldDB := dao.GormDB
	dao.GormDB = db

	cleanup := func() {
		dao.GormDB = oldDB
		_ = sqlDB.Close()
	}
	return mock, cleanup
}

func TestGenerateToken(t *testing.T) {
	InitJWT(config.JWTConfig{Secret: []byte("test-secret")})

	tokenString, err := generateToken("U001")
	if err != nil {
		t.Fatalf("generateToken() error = %v", err)
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		return []byte("test-secret"), nil
	})
	if err != nil {
		t.Fatalf("parse generated token: %v", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		t.Fatalf("claims type = %T, want jwt.MapClaims", token.Claims)
	}
	if claims["user_uuid"] != "U001" {
		t.Fatalf("user_uuid = %v, want U001", claims["user_uuid"])
	}
	if exp, ok := claims["exp"].(float64); !ok || time.Unix(int64(exp), 0).Before(time.Now()) {
		t.Fatalf("exp = %v, want a future time", claims["exp"])
	}
}

func TestLoginUserNotFound(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `user_info` WHERE telephone = ?")).
		WithArgs("13800138000").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	result, err := Login(&request.LoginRequest{
		Telephone: "13800138000",
		Password:  "password123",
	})

	if result != nil {
		t.Fatalf("result = %+v, want nil", result)
	}
	if err != ErrUserNotFound {
		t.Fatalf("error = %v, want %v", err, ErrUserNotFound)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoginInvalidPassword(t *testing.T) {
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `user_info` WHERE telephone = ?")).
		WithArgs("13800138000").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `user_info` WHERE telephone = ? ORDER BY `user_info`.`id` LIMIT ?")).
		WithArgs("13800138000", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "nickname", "telephone", "password_hash", "created_at"}).
			AddRow(1, "U001", "alice", "13800138000", passwordHash("another-password"), time.Now()))

	result, err := Login(&request.LoginRequest{
		Telephone: "13800138000",
		Password:  "password123",
	})

	if result != nil {
		t.Fatalf("result = %+v, want nil", result)
	}
	if err != ErrInvalidPassword {
		t.Fatalf("error = %v, want %v", err, ErrInvalidPassword)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoginSuccess(t *testing.T) {
	InitJWT(config.JWTConfig{Secret: []byte("test-secret")})
	mock, cleanup := newMockGormDB(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT count(*) FROM `user_info` WHERE telephone = ?")).
		WithArgs("13800138000").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `user_info` WHERE telephone = ? ORDER BY `user_info`.`id` LIMIT ?")).
		WithArgs("13800138000", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "uuid", "nickname", "telephone", "password_hash", "created_at"}).
			AddRow(1, "U001", "alice", "13800138000", passwordHash("password123"), time.Now()))

	result, err := Login(&request.LoginRequest{
		Telephone: "13800138000",
		Password:  "password123",
	})

	if err != nil {
		t.Fatalf("Login() error = %v", err)
	}
	if result == nil || result.UUID != "U001" || result.Nickname != "alice" || result.Token == "" {
		t.Fatalf("result = %+v, want uuid, nickname and token", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
