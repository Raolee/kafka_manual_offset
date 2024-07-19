package api

import (
	"kakfa-manual-offset/group_consumer"
	"kakfa-manual-offset/standalone_consumer"
	"net/http"

	"github.com/gin-gonic/gin"
)

// 괜히 만들었네 ㅋㅋ
type API struct {
	standAloneManager *standalone_consumer.Manager
	groupManager      *group_consumer.Manager
}

func NewAPI(standAloneManager *standalone_consumer.Manager, groupManager *group_consumer.Manager) *API {
	return &API{
		standAloneManager: standAloneManager,
		groupManager:      groupManager,
	}
}

func (api *API) SetOffsetHandler(c *gin.Context) {
	type Request struct {
		Topic     string `json:"topic"`
		Group     string `json:"group"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(req.Group) > 0 {
		err := api.groupManager.SetOffset(req.Topic, req.Group, req.Offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	} else {
		err := api.standAloneManager.SetOffset(req.Topic, req.Partition, req.Offset)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "offset set"})
}
