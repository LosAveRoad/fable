package request

type CreateGroupRequest struct {
	Name    string `json:"name" binding:"required"`
	AddMode int8   `json:"add_mode"`
}

type GroupIDRequest struct {
	GroupID string `json:"group_id" binding:"required"`
}

type ApproveGroupJoinRequest struct {
	GroupID     string `json:"group_id" binding:"required"`
	ApplicantID string `json:"applicant_id" binding:"required"`
}
