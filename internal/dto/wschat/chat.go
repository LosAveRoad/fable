package wschat

type Message struct {
	SendId    string `json:"send_id"`
	ReceiveId string `json:"receive_id"`
	Content   string `json:"content"`
}
