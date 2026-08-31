package v1

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"mychat/internal/dto/request"
	"mychat/internal/service/gormservice"
)

func currentUser(c *gin.Context) (string, bool) {
	v, ok := c.Get("user_uuid")
	id, valid := v.(string)
	return id, ok && valid && id != ""
}

func CreateGroup(c *gin.Context) {
	var req request.CreateGroupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group request"})
		return
	}
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	r, err := gormservice.CreateGroup(id, req.Name, req.AddMode)
	if err != nil {
		code := http.StatusInternalServerError
		if err == gormservice.ErrInvalidGroup {
			code = http.StatusBadRequest
		}
		if err == gormservice.ErrUserNotFound {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, r)
}

func JoinGroup(c *gin.Context)  { groupAction(c, true) }
func LeaveGroup(c *gin.Context) { groupAction(c, false) }
func groupAction(c *gin.Context, join bool) {
	var req request.GroupIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group request"})
		return
	}
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var err error
	if join {
		err = gormservice.JoinGroup(id, req.GroupID)
	} else {
		err = gormservice.LeaveGroup(id, req.GroupID)
	}
	if err != nil {
		code := http.StatusInternalServerError
		if err == gormservice.ErrGroupNotFound {
			code = http.StatusNotFound
		}
		if err == gormservice.ErrGroupJoinForbidden || err == gormservice.ErrGroupOwnerCannotLeave {
			code = http.StatusForbidden
		}
		if errors.Is(err, gormservice.ErrGroupJoinPending) {
			code = http.StatusAccepted
		}
		if err == gormservice.ErrUserNotFound {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func ApproveGroupJoin(c *gin.Context) {
	var req request.ApproveGroupJoinRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group request"})
		return
	}
	operator, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	if err := gormservice.ApproveGroupJoin(operator, req.ApplicantID, req.GroupID); err != nil {
		code := http.StatusInternalServerError
		if err == gormservice.ErrNotGroupAdmin {
			code = http.StatusForbidden
		}
		if err == gormservice.ErrGroupNotFound {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func GetGroupInfo(c *gin.Context) {
	var req request.GroupIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group request"})
		return
	}
	r, err := gormservice.GetGroup(req.GroupID)
	if err != nil {
		code := http.StatusInternalServerError
		if err == gormservice.ErrGroupNotFound {
			code = http.StatusNotFound
		}
		c.JSON(code, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, r)
}

func GetJoinedGroupList(c *gin.Context) {
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	r, err := gormservice.GetJoinedGroupList(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, r)
}
func GetOwnedGroupList(c *gin.Context) {
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	r, err := gormservice.GetOwnedGroupList(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, r)
}

func GetGroupMessageList(c *gin.Context) {
	var req request.GroupIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group request"})
		return
	}
	id, ok := currentUser(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	g, err := gormservice.GetGroup(req.GroupID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	found := false
	for _, m := range g.Members {
		if m == id {
			found = true
			break
		}
	}
	if !found {
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a group member"})
		return
	}
	messages, err := gormservice.GetGroupMessageList(id, req.GroupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, messages)
}
