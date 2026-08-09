package chatservice

import (
	"log"
	"sync"
	"sync/atomic"

	"mychat/internal/dto/wschat"
	"mychat/internal/service/gormservice"
)

const DefaultQueueSize = 256

type Server struct {
	clients map[string]*Client
	online  atomic.Int64

	inbound    chan wschat.Message
	register   chan *Client
	unregister chan *Client
	done       chan struct{}
	stopped    chan struct{}
	closeOnce  sync.Once
}

var ChatServer = NewServer(DefaultQueueSize)

func NewServer(queueSize int) *Server {
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}

	return &Server{
		clients:    make(map[string]*Client),
		inbound:    make(chan wschat.Message, queueSize),
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

		case message := <-s.inbound:
			created, err := gormservice.SendMessage(message.SendID, message.ReceiveID, message.Content)
			if err != nil {
				log.Printf("persist websocket message: %v", err)
				continue
			}

			s.deliver(s.clients[created.ReceiveID], wschat.Message{
				SendID:    created.SendID,
				ReceiveID: created.ReceiveID,
				Content:   created.Content,
			})

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

func (s *Server) submit(message wschat.Message) bool {
	select {
	case s.inbound <- message:
		return true
	case <-s.done:
		return false
	}
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
