package apphttp

import (
	"kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *cache.Client, rmq *mq.RabbitMQ) *gin.Engine {
	r := gin.Default() // Logger、Recovery

	// Define your routes here
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	return r
}
