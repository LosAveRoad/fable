package logic

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	pb "mychat/api/gen/fable/v1"
	"mychat/internal/dto/im"
	"mychat/internal/https_server"
	"mychat/internal/logic/messagestore"
	"mychat/internal/mcpserver"
	"mychat/internal/mcpserver/contract"
	"mychat/internal/model"
	"mychat/internal/service/aiservice"
	"net/http"
	"strconv"
	"time"
)

type DependencyStatus struct {
	Database bool `json:"database"`
	Routes   bool `json:"routes"`
	Push     bool `json:"push"`
}

func (s DependencyStatus) Ready() bool { return s.Database && s.Routes && s.Push }

// NewHandler leaves existing business handlers intact. Runtime installs the
// process DB before serving; Logic deliberately never installs legacy WS routes.
func NewHandler(s *Service, db *gorm.DB, probe func(context.Context) DependencyStatus) http.Handler {
	engine := https_server.NewLogicEngine(s.jwtKey, func(c *gin.Context) {
		limit := 0
		var e error
		if raw := c.Query("limit"); raw != "" {
			limit, e = strconv.Atoi(raw)
		}
		if e != nil || limit < 0 || limit > 500 {
			c.JSON(400, gin.H{"error": "invalid sync query"})
			return
		}
		page, e := s.messages.Sync(c.Request.Context(), c.GetString("user_uuid"), c.Query("conversation_id"), c.Query("cursor"), limit)
		if e != nil {
			code := http.StatusServiceUnavailable
			message := "message storage unavailable"
			if errors.Is(e, messagestore.ErrInvalid) {
				code = 400
				message = "invalid sync query"
			} else if errors.Is(e, messagestore.ErrForbidden) {
				code = 403
				message = "conversation access denied"
			}
			c.JSON(code, gin.H{"error": message})
			return
		}
		c.JSON(200, page)
	})
	root := http.NewServeMux()
	root.Handle("/mcp", mcpserver.NewHTTPHandler(mcpserver.NewWithSend(func(ctx context.Context, user string, input contract.SendMessageInput) (contract.SendMessageOutput, error) {
		// Resolve presentation data before saving, so a later lookup failure never
		// hides a known commit behind an ordinary tool error.
		if db == nil {
			return contract.SendMessageOutput{}, aiservice.ErrDatabase
		}
		var sender model.UserInfo
		if e := db.WithContext(ctx).Select("uuid", "nickname").Where("uuid = ?", user).First(&sender).Error; e != nil {
			if errors.Is(e, gorm.ErrRecordNotFound) {
				return contract.SendMessageOutput{}, aiservice.ErrForbidden
			}
			return contract.SendMessageOutput{}, aiservice.ErrDatabase
		}
		result, e := s.SendAI(ctx, user, input.SessionUUID, input.ClientMessageID, input.Content)
		if e != nil {
			switch {
			case errors.Is(e, messagestore.ErrInvalid), errors.Is(e, messagestore.ErrConflict):
				return contract.SendMessageOutput{}, aiservice.ErrInvalidToolInput
			case errors.Is(e, messagestore.ErrForbidden):
				return contract.SendMessageOutput{}, aiservice.ErrForbidden
			default:
				return contract.SendMessageOutput{}, aiservice.ErrDatabase
			}
		}
		m := result.Message
		view := publicMessage(m)
		return contract.SendMessageOutput{Message: contract.Message{MessageUUID: m.MessageId, SessionUUID: m.SessionId, Sender: contract.UserSummary{UUID: m.SenderId, Name: sender.Nickname}, Type: 0, Content: m.Content, Origin: 1, CreatedAt: m.CreatedAt.AsTime().UTC().Format(time.RFC3339Nano)}, Result: &im.SendResult{ClientMessageID: result.ClientMessageId, Stored: result.Stored, Message: view, Dispatch: dispatchState(result.Dispatch), Replayed: result.Replayed}}, nil
	}), s.jwtKey))
	root.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
	root.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		state := DependencyStatus{}
		if probe != nil {
			state = probe(ctx)
		}
		w.Header().Set("Content-Type", "application/json")
		if !state.Ready() {
			w.WriteHeader(503)
		}
		_ = json.NewEncoder(w).Encode(state)
	})
	root.Handle("/", engine)
	return root
}

func dispatchState(d pb.DispatchState) im.DispatchState {
	switch d {
	case pb.DispatchState_DISPATCH_STATE_QUEUED:
		return im.DispatchQueued
	case pb.DispatchState_DISPATCH_STATE_NOT_QUEUED:
		return im.DispatchNotQueued
	default:
		return im.DispatchUnknown
	}
}
