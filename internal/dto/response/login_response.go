package response

type LoginResponse struct {
	UUID     string `json:"uuid"`
	Nickname string `json:"-"`
	Token    string `json:"token"`
}

type GetUserInfoResponse struct {
	UUID      string `json:"uuid"`
	Nickname  string `json:"nickname"`
	Telephone string `json:"telephone"`
}
