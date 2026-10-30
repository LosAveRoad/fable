// Package messagestore owns atomic message persistence and authoritative sync.
package messagestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	pb "mychat/api/gen/fable/v1"
	"mychat/internal/model"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInvalid   = errors.New("invalid message request")
	ErrForbidden = errors.New("message access forbidden")
	ErrConflict  = errors.New("message idempotency conflict")
	ErrDatabase  = errors.New("message database unavailable")
)

// Input accepts user text only. AI origin is available only through SaveAI.
type Input struct {
	SenderID, ClientMessageID string
	Destination               *pb.Destination
	Content                   string
}

type Saved struct {
	Message    *pb.MessageView
	Recipients []string
	Replayed   bool
}

type Store struct{ db *gorm.DB }

func New(db *gorm.DB) *Store { return &Store{db: db} }

func (s *Store) Save(ctx context.Context, in Input) (Saved, error) {
	if err := validateInput(in); err != nil {
		return Saved{}, err
	}
	return s.save(ctx, in, "", model.MessageOriginUser)
}

// SaveAI is an internal trusted entry point; callers must never expose origin
// selection in an external request. Authorization is still checked here.
func (s *Store) SaveAI(ctx context.Context, user, session, clientID, content string) (Saved, error) {
	user, session, content = strings.TrimSpace(user), strings.TrimSpace(session), strings.TrimSpace(content)
	if !validID(user, 'U') || !validID(session, 'S') || !validUUID(clientID) || !validText(content) || utf8.RuneCountInString(content) > 4000 {
		return Saved{}, ErrInvalid
	}
	return s.save(ctx, Input{SenderID: user, ClientMessageID: clientID, Content: content}, session, model.MessageOriginAI)
}

// fingerprintV1 fixes field order and enum spelling across all entry points.
// It deliberately excludes connection, request and dispatch metadata.
func fingerprintV1(in Input, origin int8) []byte {
	type destination struct {
		ReceiveType string `json:"receive_type"`
		ReceiveID   string `json:"receive_id"`
	}
	type request struct {
		Version     int         `json:"v"`
		Destination destination `json:"destination"`
		Content     string      `json:"content"`
		Type        string      `json:"type"`
		Origin      string      `json:"origin"`
	}
	kind, source := "user", "user"
	if in.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
		kind = "group"
	}
	if origin == model.MessageOriginAI {
		source = "ai"
	}
	b, _ := json.Marshal(request{1, destination{kind, in.Destination.ReceiveId}, in.Content, "text", source})
	sum := sha256.Sum256(b)
	return sum[:]
}

func (s *Store) save(ctx context.Context, in Input, aiSession string, origin int8) (Saved, error) {
	if s == nil || s.db == nil {
		return Saved{}, ErrDatabase
	}
	var result Saved
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Sender locking makes (sender, client ID) serialization global, including
		// two concurrent requests that select different conversations.
		if err := lockUser(tx, in.SenderID); err != nil {
			return err
		}
		var sessionID string
		var recipients []string
		if aiSession != "" {
			session, err := lockDirect(tx, in.SenderID, "", aiSession)
			if err != nil {
				return err
			}
			peer := session.SendId
			if peer == in.SenderID {
				peer = session.ReceiveId
			}
			in.Destination = &pb.Destination{ReceiveType: pb.ReceiveType_RECEIVE_TYPE_USER, ReceiveId: peer}
			sessionID = session.UUID
			recipients = []string{in.SenderID, peer}
			// A locking read observes the latest committed allow-list, even under
			// MySQL REPEATABLE READ, and blocks a concurrent revocation until commit.
			var access model.UserAISessionAccess
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("user_uuid = ? AND session_uuid = ?", in.SenderID, sessionID).Take(&access).Error; err != nil {
				return authorityError(err)
			}
		} else if in.Destination.ReceiveType == pb.ReceiveType_RECEIVE_TYPE_GROUP {
			group, members, err := lockGroup(tx, in.SenderID, in.Destination.ReceiveId)
			if err != nil {
				return err
			}
			sessionID = group.UUID
			recipients = members
		} else {
			session, err := lockDirect(tx, in.SenderID, in.Destination.ReceiveId, "")
			if err != nil {
				return err
			}
			sessionID = session.UUID
			recipients = []string{in.SenderID, in.Destination.ReceiveId}
		}
		recipients = sortedUnique(recipients)
		if err := requireUsers(tx, recipients); err != nil {
			return err
		}
		fingerprint := fingerprintV1(in, origin)
		var existing message
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("send_id = ? AND client_message_id = ?", in.SenderID, in.ClientMessageID).Take(&existing).Error
		if err == nil {
			if !bytes.Equal(existing.RequestFingerprint, fingerprint) {
				return ErrConflict
			}
			snapshot, err := decodeRecipients(existing.RecipientIDs)
			if err != nil {
				return err
			}
			view, err := messageView(existing)
			if err != nil {
				return err
			}
			result = Saved{Message: view, Recipients: snapshot, Replayed: true}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrDatabase
		}
		snapshot, err := json.Marshal(recipients)
		if err != nil {
			return ErrDatabase
		}
		row := message{UUID: "M" + uuid.NewString(), SessionID: sessionID, Type: model.MessageTypeText, Content: in.Content, Origin: origin, SendID: in.SenderID, ReceiveID: in.Destination.ReceiveId, CreatedAt: tx.NowFunc().UTC().Truncate(time.Microsecond), ClientMessageID: in.ClientMessageID, RequestFingerprint: fingerprint, RecipientIDs: datatypes.JSON(snapshot)}
		if err := tx.Create(&row).Error; err != nil {
			return ErrDatabase
		}
		view, err := messageView(row)
		if err != nil {
			return err
		}
		result = Saved{Message: view, Recipients: recipients}
		return nil
	})
	if err != nil {
		return Saved{}, storeError(err)
	}
	return result, nil
}

func authorityError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrForbidden
	}
	return ErrDatabase
}
func storeError(err error) error {
	if errors.Is(err, ErrInvalid) || errors.Is(err, ErrForbidden) || errors.Is(err, ErrConflict) {
		return err
	}
	return ErrDatabase
}
func lockUser(tx *gorm.DB, user string) error {
	var row model.UserInfo
	return authorityErrorOrNil(tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id", "uuid").Where("uuid = ?", user).Take(&row).Error)
}
func authorityErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return authorityError(err)
}

func lockDirect(tx *gorm.DB, user, peer, sessionID string) (model.Session, error) {
	var session model.Session
	q := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("type = ?", model.SessionTypeUser)
	if sessionID != "" {
		q = q.Where("uuid = ?", sessionID)
	} else {
		q = q.Where("((send_id = ? AND receive_id = ?) OR (send_id = ? AND receive_id = ?))", user, peer, peer, user)
	}
	if err := q.Take(&session).Error; err != nil {
		return session, authorityError(err)
	}
	if !validID(session.UUID, 'S') || !validID(session.SendId, 'U') || !validID(session.ReceiveId, 'U') || session.SendId == session.ReceiveId {
		return session, ErrDatabase
	}
	if session.SendId != user && session.ReceiveId != user {
		return session, ErrForbidden
	}
	return session, nil
}

func lockGroup(tx *gorm.DB, user, id string) (model.GroupInfo, []string, error) {
	var group model.GroupInfo
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("uuid = ?", id).Take(&group).Error; err != nil {
		return group, nil, authorityError(err)
	}
	if group.Status != model.GroupStatusNormal {
		return group, nil, ErrForbidden
	}
	var members []string
	if json.Unmarshal(group.Members, &members) != nil || len(members) == 0 {
		return group, nil, ErrDatabase
	}
	found := false
	for _, member := range members {
		if !validID(member, 'U') {
			return group, nil, ErrDatabase
		}
		if member == user {
			found = true
		}
	}
	if !found {
		return group, nil, ErrForbidden
	}
	return group, sortedUnique(members), nil
}

func sortedUnique(ids []string) []string {
	result := append([]string(nil), ids...)
	sort.Strings(result)
	n := 0
	for _, id := range result {
		if n == 0 || id != result[n-1] {
			result[n] = id
			n++
		}
	}
	return result[:n]
}
func decodeRecipients(raw []byte) ([]string, error) {
	var ids []string
	if json.Unmarshal(raw, &ids) != nil || len(ids) == 0 {
		return nil, ErrDatabase
	}
	for _, id := range ids {
		if !validID(id, 'U') {
			return nil, ErrDatabase
		}
	}
	sorted := sortedUnique(ids)
	if len(sorted) != len(ids) {
		return nil, ErrDatabase
	}
	for i := range ids {
		if ids[i] != sorted[i] {
			return nil, ErrDatabase
		}
	}
	return ids, nil
}
func requireUsers(tx *gorm.DB, ids []string) error {
	// Bound query placeholders for large group snapshots.
	for first := 0; first < len(ids); first += 100 {
		last := first + 100
		if last > len(ids) {
			last = len(ids)
		}
		var count int64
		if err := tx.Model(&model.UserInfo{}).Where("uuid IN ?", ids[first:last]).Count(&count).Error; err != nil {
			return ErrDatabase
		}
		if count != int64(last-first) {
			return ErrForbidden
		}
	}
	return nil
}
