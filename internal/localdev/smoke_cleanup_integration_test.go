//go:build integration

package localdev

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	pb "mychat/api/gen/fable/v1"
	logicapp "mychat/internal/logic/app"
	"mychat/internal/logic/messagestore"
)

type smokeCleanupFixture struct {
	db, cleanup  *sql.DB
	storeDB      *gorm.DB
	users, ids   [2]string
	session      string
	connectionID int64
}

func newSmokeCleanupFixture(t *testing.T) *smokeCleanupFixture {
	t.Helper()
	file := os.Getenv("FABLE_LOCAL_SMOKE_CONFIG")
	if file == "" {
		t.Skip("selected isolated local MySQL config required")
	}
	c, err := logicapp.LoadConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if !databaseName.MatchString(c.Base.MySQLConfig.DatabaseName) || !loopback(c.Base.MySQLConfig.Host) {
		t.Fatal("isolated local DB required")
	}
	cfg := mysqlDriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.Base.MySQLConfig.Host, strconv.Itoa(c.Base.MySQLConfig.Port))
	cfg.User = c.Base.MySQLConfig.User
	cfg.Passwd = c.Base.MySQLConfig.Password
	cfg.DBName = c.Base.MySQLConfig.DatabaseName
	cfg.ParseTime = true
	// The reviewer race depends on nonmatching DELETEs taking no gap locks.
	cfg.Params = map[string]string{"transaction_isolation": "'READ-COMMITTED'"}
	f := &smokeCleanupFixture{users: [2]string{"U" + uuid.NewString(), "U" + uuid.NewString()}, ids: [2]string{uuid.NewString(), uuid.NewString()}, session: "S" + uuid.NewString()}
	f.db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.db.Close() })
	f.cleanup, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	f.cleanup.SetMaxOpenConns(1)
	t.Cleanup(func() { f.cleanup.Close() })
	if err = f.cleanup.QueryRow("SELECT CONNECTION_ID()").Scan(&f.connectionID); err != nil {
		t.Fatal(err)
	}
	var isolation string
	if err = f.cleanup.QueryRow("SELECT @@transaction_isolation").Scan(&isolation); err != nil || isolation != "READ-COMMITTED" {
		t.Fatalf("isolation=%s err=%v", isolation, err)
	}
	f.storeDB, err = gorm.Open(gormmysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	storeSQL, err := f.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storeSQL.Close() })
	for i, user := range f.users {
		if _, err = f.db.Exec("INSERT INTO user_info (uuid,nickname,telephone,password_hash,created_at) VALUES (?,?,?,?,UTC_TIMESTAMP(6))", user, "cleanup-race", f.ids[i][:24], "fixture-only"); err != nil {
			t.Fatal(err)
		}
	}
	a, b := f.users[0], f.users[1]
	if a > b {
		a, b = b, a
	}
	if _, err = f.db.Exec("INSERT INTO session (uuid,send_id,receive_id,type,created_at) VALUES (?,?,?,0,UTC_TIMESTAMP(6))", f.session, a, b); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Exact own random IDs also recover the intentionally failing RED case.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := cleanupSmoke(ctx, f.db, f.users, f.ids, f.session); err != nil {
			t.Errorf("fixture cleanup: %v", err)
		}
	})
	return f
}

func (f *smokeCleanupFixture) input(sender int) messagestore.Input {
	return messagestore.Input{SenderID: f.users[sender], ClientMessageID: f.ids[sender], Destination: &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: f.users[1-sender]}, Content: "cleanup concurrency fixture"}
}

func (f *smokeCleanupFixture) assertEmpty(t *testing.T) {
	t.Helper()
	var users, sessions, messages int
	if err := f.db.QueryRow("SELECT (SELECT COUNT(*) FROM user_info WHERE uuid IN (?,?)),(SELECT COUNT(*) FROM session WHERE uuid=?),(SELECT COUNT(*) FROM message WHERE session_id=? AND send_id IN (?,?) AND client_message_id IN (?,?))", f.users[0], f.users[1], f.session, f.session, f.users[0], f.users[1], f.ids[0], f.ids[1]).Scan(&users, &sessions, &messages); err != nil {
		t.Fatal(err)
	}
	if users != 0 || sessions != 0 || messages != 0 {
		t.Fatalf("own fixture rows remain: users=%d sessions=%d messages=%d", users, sessions, messages)
	}
}

func TestSmokeCleanupWaitsForInflightSaveUnderReadCommitted(t *testing.T) {
	for sender := 0; sender < 2; sender++ {
		t.Run(strconv.Itoa(sender), func(t *testing.T) {
			f := newSmokeCleanupFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			locked, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			if err := f.storeDB.Callback().Create().Before("gorm:create").Register("test:pause_authorized_smoke_save", func(tx *gorm.DB) {
				if tx.Statement.Table == "message" {
					close(locked)
					select {
					case <-release:
					case <-ctx.Done():
						tx.AddError(ctx.Err())
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			saved := make(chan error, 1)
			go func() { _, err := messagestore.New(f.storeDB).Save(ctx, f.input(sender)); saved <- err }()
			select {
			case <-locked:
			case <-ctx.Done():
				t.Fatal("Save did not acquire authority locks")
			}
			cleaned := make(chan error, 1)
			go func() { cleaned <- cleanupSmoke(ctx, f.cleanup, f.users, f.ids, f.session) }()
			// Observe a real InnoDB wait, not a scheduler sleep, before permitting INSERT.
			blocked := false
			for !blocked && ctx.Err() == nil {
				var n int
				if err := f.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.threads th ON th.THREAD_ID=w.REQUESTING_THREAD_ID WHERE th.PROCESSLIST_ID=?", f.connectionID).Scan(&n); err != nil {
					t.Fatal(err)
				}
				blocked = n > 0
				if !blocked {
					time.Sleep(5 * time.Millisecond)
				}
			}
			unblock()
			saveErr, cleanupErr := <-saved, <-cleaned
			if !blocked {
				t.Fatal("cleanup never waited on Save's authority transaction")
			}
			if saveErr != nil || cleanupErr != nil {
				t.Fatalf("Save=%v cleanup=%v", saveErr, cleanupErr)
			}
			f.assertEmpty(t)
		})
	}
}

func TestSmokeCleanupWinningRejectsConcurrentSave(t *testing.T) {
	f := newSmokeCleanupFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Hold only the session to pause cleanup after it has taken both user gates.
	blocker, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	var id string
	if err = blocker.QueryRowContext(ctx, "SELECT uuid FROM session WHERE uuid=? FOR UPDATE", f.session).Scan(&id); err != nil {
		t.Fatal(err)
	}
	cleaned := make(chan error, 1)
	go func() { cleaned <- cleanupSmoke(ctx, f.cleanup, f.users, f.ids, f.session) }()
	waitSmokeLockTable(t, ctx, f.db, f.connectionID, "session")
	storeSQL, err := f.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	storeSQL.SetMaxOpenConns(1)
	var saveConnection int64
	if err = storeSQL.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&saveConnection); err != nil {
		t.Fatal(err)
	}
	saved := make(chan error, 1)
	go func() { _, err := messagestore.New(f.storeDB).Save(ctx, f.input(0)); saved <- err }()
	waitSmokeLockTable(t, ctx, f.db, saveConnection, "user_info")
	if err = blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	cleanupErr, saveErr := <-cleaned, <-saved
	if cleanupErr != nil {
		t.Fatal(cleanupErr)
	}
	if !errors.Is(saveErr, messagestore.ErrForbidden) {
		t.Fatalf("Save after cleanup authority gate = %v", saveErr)
	}
	// Both directions must continue to fail after the fixture authority is gone.
	for sender := 0; sender < 2; sender++ {
		if _, err := messagestore.New(f.storeDB).Save(ctx, f.input(sender)); !errors.Is(err, messagestore.ErrForbidden) {
			t.Fatalf("Save after authority removal = %v", err)
		}
	}
	f.assertEmpty(t)
}

func waitSmokeLockTable(t *testing.T, ctx context.Context, db *sql.DB, connection int64, table string) {
	t.Helper()
	for ctx.Err() == nil {
		var n int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.threads th ON th.THREAD_ID=w.REQUESTING_THREAD_ID JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID AND l.ENGINE=w.ENGINE WHERE th.PROCESSLIST_ID=? AND l.OBJECT_NAME=?", connection, table).Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("connection did not wait on %s", table)
}
