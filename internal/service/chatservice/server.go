package chatservice

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"mychat/internal/dto/response"
	"mychat/internal/dto/wschat"
	"mychat/internal/service/gormservice"
)

const DefaultQueueSize = 256

type Server struct {
	clients map[string]*Client
	online  atomic.Int64

	routeQueue chan routeRequest
	register   chan *Client
	unregister chan *Client
	done       chan struct{}
	stopped    chan struct{}
	closeOnce  sync.Once
}

type routeRequest struct {
	recipients []string
	message    wschat.Message
}

var ChatServer = NewServer(DefaultQueueSize)

func NewServer(queueSize int) *Server {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}

	return &Server{
		clients:    make(map[string]*Client),
		routeQueue: make(chan routeRequest, queueSize),
		register:   make(chan *Client),
		unregister: make(chan *Client, queueSize),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
}

func (s *Server) Start() {
	defer close(s.stopped)

	for {
		select {
		case client := <-s.register:
			if client == nil {
				continue
			}

			previous, existed := s.clients[client.userUUID]
			s.clients[client.userUUID] = client
			if !existed {
				s.online.Add(1)
			}

			if previous != nil && previous != client {
				go previous.Close()
			}

		case client := <-s.unregister:
			s.removeClient(client)

		case request := <-s.routeQueue:
			for _, userUUID := range request.recipients {
				s.deliver(s.clients[userUUID], request.message)
			}

		case <-s.done:
			s.closeClients()
			return
		}
	}
}

func (s *Server) Register(client *Client) bool {
	if client == nil {
		return false
	}

	select {
	case s.register <- client:
		return true
	case <-s.done:
		return false
	}
}

func (s *Server) unregisterClient(client *Client) {
	if client == nil {
		return
	}

	select {
	case s.unregister <- client:
	case <-s.done:
	}
}

func (s *Server) RouteTo(userUUID string, message wschat.Message) bool {
	if userUUID == "" {
		return false
	}

	select {
	case s.routeQueue <- routeRequest{recipients: []string{userUUID}, message: message}:
		return true
	case <-s.done:
		return false
	}
}

// RouteToUsers broadcasts one persisted message to each distinct online user.
// Delivery is best effort; a slow client is handled by deliver without
// blocking other recipients.
func (s *Server) RouteToUsers(userUUIDs []string, message wschat.Message) bool {
	if len(userUUIDs) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(userUUIDs))
	recipients := make([]string, 0, len(userUUIDs))
	for _, id := range userUUIDs {
		if id != "" {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				recipients = append(recipients, id)
			}
		}
	}
	if len(recipients) == 0 {
		return false
	}
	select {
	case s.routeQueue <- routeRequest{recipients: recipients, message: message}:
		return true
	case <-s.done:
		return false
	}
}

// HandleMessage is shared by Channel and Kafka modes. Only the transport that
// delivered the event changes; persistence, authorization and fan-out remain
// identical.
func (s *Server) HandleMessage(senderID string, incoming wschat.Message) error {
	if senderID == "" || incoming.SendID != senderID || !validDestination(incoming.ReceiveID, incoming.ReceiveType) {
		return gormservice.ErrInvalidGroup
	}
	var created response.MessageResponse
	var err error
	if incoming.ReceiveType == wschat.ReceiveTypeGroup || strings.HasPrefix(incoming.ReceiveID, "G") {
		created, err = gormservice.SendGroupMessage(senderID, incoming.ReceiveID, incoming.Content)
	} else {
		created, err = gormservice.SendMessage(senderID, incoming.ReceiveID, incoming.Content)
	}
	if err != nil {
		return err
	}
	if PublishChatEvent != nil {
		return PublishChatEvent(context.Background(), wschat.ChatEvent{EventID: created.UUID, SenderID: created.SendID, ReceiveID: created.ReceiveID, ReceiveType: incoming.ReceiveType, Content: created.Content, Origin: created.Origin, CreatedAt: created.CreatedAt})
	}
	return s.DeliverEvent(wschat.ChatEvent{SenderID: created.SendID, ReceiveID: created.ReceiveID, ReceiveType: incoming.ReceiveType, Content: created.Content, Origin: created.Origin})
}

// DeliverEvent performs only process-local WebSocket delivery. Persistence is
// done exactly once by the pod that accepted the sender's message.
func (s *Server) DeliverEvent(event wschat.ChatEvent) error {
	outgoing := wschat.Message{SendID: event.SenderID, ReceiveID: event.ReceiveID, Content: event.Content, Origin: event.Origin, ReceiveType: event.ReceiveType}
	if event.ReceiveType == wschat.ReceiveTypeGroup {
		group, err := gormservice.GetGroup(event.ReceiveID)
		if err != nil {
			return err
		}
		if !s.RouteToUsers(group.Members, outgoing) {
			return gormservice.ErrDatabase
		}
		return nil
	}
	// Offline users are normal; history remains in MySQL for reconnects.
	s.RouteTo(event.ReceiveID, outgoing)
	return nil
}

func (s *Server) removeClient(client *Client) {
	if client == nil {
		return
	}

	if current, ok := s.clients[client.userUUID]; ok && current == client {
		delete(s.clients, client.userUUID)
		s.online.Add(-1)
	}
}

func (s *Server) deliver(client *Client, message wschat.Message) bool {
	if client == nil {
		return false
	}

	select {
	case <-client.done:
		return false
	default:
	}

	select {
	case client.outbound <- message:
		return true
	case <-client.done:
		return false
	default:
		go client.Close()
		return false
	}
}

func (s *Server) OnlineCount() int {
	return int(s.online.Load())
}

func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})

	<-s.stopped
}

func (s *Server) closeClients() {
	clients := make([]*Client, 0, len(s.clients))
	for _, client := range s.clients {
		clients = append(clients, client)
	}
	clear(s.clients)
	s.online.Store(0)

	for _, client := range clients {
		client.Close()
	}
}
