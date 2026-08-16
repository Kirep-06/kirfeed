package apphttp

import (
	"kirfeed/internal/account"
	rediscache "kirfeed/internal/infra/cache"
	jwtmiddleware "kirfeed/internal/infra/jwt"
	"kirfeed/internal/infra/mq"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *mq.RabbitMQ) *gin.Engine {
	r := gin.Default() // Logger、Recovery

	// ping -----> pong!
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// account
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", accountHandler.Register)
		accountGroup.POST("/login", accountHandler.Login)
		accountGroup.POST("/refresh", accountHandler.Refresh)
		accountGroup.POST("/logout", jwtmiddleware.JWTAuth(accountRepository, cache), accountHandler.Logout)
	}

	return r
}
