package wschat

type Message struct {
	SendID    string `json:"send_id"`
	ReceiveID string `json:"receive_id"`
	Content   string `json:"content"`
	Origin    int8   `json:"origin"`
}
