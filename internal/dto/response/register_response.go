package response

type RegisterResponse struct {
	UUID      string `json:"uuid"`
	Nickname  string `json:"nickname"`
	Telephone string `json:"telephone"`
}
