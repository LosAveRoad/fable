package localdev

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"mychat/internal/dto/im"
	logicapp "mychat/internal/logic/app"
)

var errSmoke = errors.New("selected topology smoke failed; inspect local role readiness and private logs")

// Smoke proves the selected ingress, Logic, Kafka, Job and Comet can persist and
// deliver in both directions. It creates no schema and deletes only its fresh
// random fixture identities. Tokens, contents and backend errors are never logged.
func Smoke(ctx context.Context, c logicapp.Config, entry string) (result error) {
	u, err := url.Parse(entry)
	if err != nil || !databaseName.MatchString(c.Base.MySQLConfig.DatabaseName) || u.Scheme != "http" || !loopback(u.Hostname()) || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Port() == "" {
		return errSmoke
	}
	if !loopback(c.Base.MySQLConfig.Host) || len(c.Base.JWTConfig.Secret) < 32 {
		return errSmoke
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	cfg := mysqlDriver.NewConfig()
	cfg.User = c.Base.MySQLConfig.User
	cfg.Passwd = c.Base.MySQLConfig.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.Base.MySQLConfig.Host, strconv.Itoa(c.Base.MySQLConfig.Port))
	cfg.DBName = c.Base.MySQLConfig.DatabaseName
	cfg.Timeout = 2 * time.Second
	cfg.ReadTimeout = 2 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return errSmoke
	}
	defer db.Close()
	users := [2]string{"U" + uuid.NewString(), "U" + uuid.NewString()}
	ids := [2]string{uuid.NewString(), uuid.NewString()}
	session := "S" + uuid.NewString()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		// All predicates include our random IDs; no broad user/session deletion.
		if err := cleanupSmoke(cleanup, db, users, ids, session); err != nil {
			result = errSmoke
		}
	}()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errSmoke
	}
	defer tx.Rollback()
	for i, user := range users {
		if _, err = tx.ExecContext(ctx, "INSERT INTO user_info (uuid,nickname,telephone,password_hash,created_at) VALUES (?,?,?,?,UTC_TIMESTAMP(6))", user, "local-smoke", ids[i][:24], "fixture-only"); err != nil {
			return errSmoke
		}
	}
	a, b := users[0], users[1]
	if a > b {
		a, b = b, a
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO session (uuid,send_id,receive_id,type,created_at) VALUES (?,?,?,0,UTC_TIMESTAMP(6))", session, a, b); err != nil {
		return errSmoke
	}
	if err = tx.Commit(); err != nil {
		return errSmoke
	}
	var sockets [2]*websocket.Conn
	for i, user := range users {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_uuid": user, "exp": time.Now().Add(time.Minute).Unix()}).SignedString(c.Base.JWTConfig.Secret)
		if err != nil {
			return errSmoke
		}
		target := *u
		target.Scheme = "ws"
		target.Path = "/wss"
		target.RawPath = ""
		q := url.Values{"protocol": {"v2"}, "token": {token}}
		target.RawQuery = q.Encode()
		dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second, Proxy: nil}
		socket, response, err := dialer.DialContext(ctx, target.String(), http.Header{"Origin": {entry}})
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if err != nil {
			return errSmoke
		}
		sockets[i] = socket
		defer socket.Close()
		stopCancellation := context.AfterFunc(ctx, func() { socket.Close() })
		defer stopCancellation()
		deadline, _ := ctx.Deadline()
		socket.SetReadDeadline(deadline)
		socket.SetWriteDeadline(deadline)
		socket.SetReadLimit(65536)
	}
	// Connecting both via ingress also exercises round-robin Comet selection.
	for i := range users {
		sender, peer := sockets[i], sockets[1-i]
		if err = sender.WriteJSON(im.SendFrame{Event: im.EventSendMessage, ClientMessageID: ids[i], ReceiveID: users[1-i], ReceiveType: im.ReceiveTypeUser, Content: "local smoke"}); err != nil {
			return errSmoke
		}
		var saved im.Message
		for {
			_, raw, e := sender.ReadMessage()
			if e != nil {
				return errSmoke
			}
			var frame im.SendResultFrame
			if json.Unmarshal(raw, &frame) != nil {
				return errSmoke
			}
			if frame.Event != im.EventSendResult || frame.ClientMessageID != ids[i] {
				continue
			}
			if frame.Error != nil || frame.Result == nil || !frame.Result.Stored || frame.Result.Dispatch != im.DispatchQueued {
				return errSmoke
			}
			saved = frame.Result.Message
			if saved.MessageID == "" || saved.SendID != users[i] || saved.ReceiveID != users[1-i] || saved.SessionID != session {
				return errSmoke
			}
			break
		}
		for {
			_, raw, e := peer.ReadMessage()
			if e != nil {
				return errSmoke
			}
			var frame im.MessageFrame
			if json.Unmarshal(raw, &frame) != nil {
				return errSmoke
			}
			if frame.Event != im.EventMessage || frame.Message.MessageID != saved.MessageID {
				continue
			}
			if frame.Message.SendID != users[i] || frame.Message.ReceiveID != users[1-i] || frame.Message.SessionID != session {
				return errSmoke
			}
			break
		}
		var count int
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM message WHERE uuid=? AND session_id=? AND send_id=? AND receive_id=? AND client_message_id=?", saved.MessageID, session, users[i], users[1-i], ids[i]).Scan(&count); err != nil || count != 1 {
			return errSmoke
		}
	}
	return nil
}

func loopback(host string) bool {
	return host == "localhost" || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())
}

func cleanupSmoke(ctx context.Context, db *sql.DB, users, ids [2]string, session string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	a, b := users[0], users[1]
	if a > b {
		a, b = b, a
	}
	// Save locks its sender before session/message access. Take both fixture
	// authority gates first, in canonical order, so a Save already authorized
	// must finish before our DELETEs, and any later Save sees a missing sender.
	for _, user := range []string{a, b} {
		var locked string
		err := tx.QueryRowContext(ctx, "SELECT uuid FROM user_info WHERE uuid=? FOR UPDATE", user).Scan(&locked)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	for i := range users {
		if _, err = tx.ExecContext(ctx, "DELETE FROM message WHERE session_id=? AND send_id=? AND receive_id=? AND client_message_id=?", session, users[i], users[1-i], ids[i]); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM session WHERE uuid=? AND send_id=? AND receive_id=?", session, a, b); err != nil {
		return err
	}
	for _, user := range users {
		if _, err = tx.ExecContext(ctx, "DELETE FROM user_info WHERE uuid=?", user); err != nil {
			return err
		}
	}
	return tx.Commit()
}
