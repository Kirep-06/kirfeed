package apphttp

import (
	"kirfeed/internal/account"
	"kirfeed/internal/feed"
	rediscache "kirfeed/internal/infra/cache"
	jwtmiddleware "kirfeed/internal/infra/jwt"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/video"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *mq.RabbitMQ) *gin.Engine {
	r := gin.Default() // Logger、Recovery
	if err := r.SetTrustedProxies(nil); err != nil {
		panic(err)
	}
	r.Static("/static", "./.run/uploads")

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
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
		accountGroup.POST("/logout", jwtmiddleware.JWTAuth(accountRepository, cache), accountHandler.Logout)
		accountGroup.POST("/rename", jwtmiddleware.JWTAuth(accountRepository, cache), accountHandler.Rename)
		accountGroup.POST("/uploadAvatar", jwtmiddleware.JWTAuth(accountRepository, cache), accountHandler.UploadAvatar)
		accountGroup.POST("/updateProfile", jwtmiddleware.JWTAuth(accountRepository, cache), accountHandler.UpdateProfile)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
	}

	// video
	videoRepository := video.NewVideoRepository(db)
	videoService := video.NewVideoService(videoRepository, cache)
	videoHandler := video.NewVideoHandler(videoService, accountService)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/detail", videoHandler.GetDetail)
		authorized := videoGroup.Group("", jwtmiddleware.JWTAuth(accountRepository, cache))
		authorized.POST("/publish", videoHandler.PublishVideo)
		authorized.POST("/uploadVideo", videoHandler.UploadVideo)
		authorized.POST("/uploadCover", videoHandler.UploadCover)
		authorized.POST("/delete", videoHandler.DeleteVideo)
	}

	// feed
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)
	r.POST("/feed/latest", feedHandler.ListLatest)

	return r
}
