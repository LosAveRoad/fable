package wschat

type Message struct {
	SendID    string `json:"send_id"`
	ReceiveID string `json:"receive_id"`
	Content   string `json:"content"`
	Origin    int8   `json:"origin"`
	// ReceiveType is 0 for a user and 1 for a group. Empty/zero preserves the
	// existing direct-message wire format.
	ReceiveType int8 `json:"receive_type,omitempty"`
}

const (
	ReceiveTypeUser int8 = iota
	ReceiveTypeGroup
)
