package chatservice

import (
	"sync"

	"mychat/internal/dto/wschat"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn     *websocket.Conn
	server   *Server
	userUUID string
	outbound chan wschat.Message

	done      chan struct{}
	closeOnce sync.Once
}

func NewClient(server *Server, conn *websocket.Conn, userUUID string, outboundBuffer int) *Client {
	if outboundBuffer <= 0 {
		outboundBuffer = DefaultQueueSize
	}

	return &Client{
		conn:     conn,
		server:   server,
		userUUID: userUUID,
		outbound: make(chan wschat.Message, outboundBuffer),
		done:     make(chan struct{}),
	}
}

func (c *Client) Start() bool {
	if c.server == nil || c.conn == nil || c.userUUID == "" || !c.server.Register(c) {
		c.Close()
		return false
	}

	go c.read()
	go c.write()
	return true
}

func (c *Client) read() {
	defer c.Close()

	for {
		var message wschat.Message
		if err := c.conn.ReadJSON(&message); err != nil {
			return
		}
		if message.SendID != c.userUUID {
			return
		}

		if !c.server.submit(message) {
			return
		}
	}
}

func (c *Client) write() {
	defer c.Close()

	for {
		select {
		case message := <-c.outbound:
			if err := c.conn.WriteJSON(message); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
		if c.server != nil {
			c.server.unregisterClient(c)
		}
	})
}
