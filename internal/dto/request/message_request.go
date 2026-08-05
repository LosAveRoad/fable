package request

type GetMessageListRequest struct {
	UserOneID string `json:"user_one_id" binding:"required"`
	UserTwoID string `json:"user_two_id" binding:"required"`
}
