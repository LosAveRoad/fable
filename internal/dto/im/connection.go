package im

import "time"

// ConnectionIdentity is internal-only, used by Redis and Kafka route hints.
// Never embed it in a public message, sync result, or WebSocket frame.
type ConnectionIdentity struct {
	UserID       string `json:"user_id"`
	InstanceID   string `json:"instance_id"`
	ConnectionID string `json:"connection_id"`
	RouteToken   string `json:"route_token"`
}

type CometIdentity struct {
	InstanceID string `json:"instance_id"`
	BootID     string `json:"boot_id"`
}

type RouteLease struct {
	Identity      ConnectionIdentity `json:"identity"`
	AuthExpiresAt time.Time          `json:"auth_expires_at"`
	LeaseDeadline time.Time          `json:"lease_deadline"`
}

type CometLease struct {
	Identity      CometIdentity `json:"identity"`
	RPCAddress    string        `json:"rpc_address"`
	Draining      bool          `json:"draining"`
	LeaseDeadline time.Time     `json:"lease_deadline"`
}
