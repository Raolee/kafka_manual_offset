package api

import (
	"kakfa-manual-offset/standalone_consumer"
	"net/http"

	"github.com/gin-gonic/gin"
)

// 괜히 만들었네 ㅋㅋ
type API struct {
	manager *standalone_consumer.Manager
}

func NewAPI(manager *standalone_consumer.Manager) *API {
	return &API{manager: manager}
}

func (api *API) SetOffsetHandler(c *gin.Context) {
	type Request struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}
	var req Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := api.manager.SetOffset(req.Topic, req.Partition, req.Offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "offset set"})
}
