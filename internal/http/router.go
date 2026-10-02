package apphttp

import (
	"kirfeed/internal/account"
	"kirfeed/internal/feed"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/jwt"
	"kirfeed/internal/infra/mq"
	"kirfeed/internal/social"
	"kirfeed/internal/video"
	"log"
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
		accountGroup.POST("/logout", jwt.JWTAuth(accountRepository, cache), accountHandler.Logout)
		accountGroup.POST("/rename", jwt.JWTAuth(accountRepository, cache), accountHandler.Rename)
		accountGroup.POST("/uploadAvatar", jwt.JWTAuth(accountRepository, cache), accountHandler.UploadAvatar)
		accountGroup.POST("/updateProfile", jwt.JWTAuth(accountRepository, cache), accountHandler.UpdateProfile)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
	}

	// video
	videoRepository := video.NewVideoRepository(db)
	videoService := video.NewVideoService(videoRepository, cache)
	videoHandler := video.NewVideoHandler(videoService, accountService)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/getDetail", videoHandler.GetDetail)
	}
	protectedVideoGroup := videoGroup.Group("")
	protectedVideoGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedVideoGroup.POST("/uploadVideo", videoHandler.UploadVideo)
		protectedVideoGroup.POST("/uploadCover", videoHandler.UploadCover)
		protectedVideoGroup.POST("/publish", videoHandler.PublishVideo)
		protectedVideoGroup.POST("/delete", videoHandler.DeleteVideo)
	}
	// like
	likeMQ, err := mq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	popularityMQ, err := mq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}
	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ)
	likeHandler := video.NewLikeHandler(likeService)
	likeGroup := r.Group("/like")
	protectedLikeGroup := likeGroup.Group("")
	protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedLikeGroup.POST("/like", likeHandler.Like)
		protectedLikeGroup.POST("/unlike", likeHandler.Unlike)
		protectedLikeGroup.POST("/isLiked", likeHandler.IsLiked)
		protectedLikeGroup.POST("/listMyLikedVideos", likeHandler.ListMyLikedVideos)
	}
	commentMQ, err := mq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentRepository := video.NewCommentRepository(db)
	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)
	commentGroup := r.Group("/comment")
	commentGroup.POST("/listAll", commentHandler.GetAllComments)
	protectedCommentGroup := commentGroup.Group("")
	protectedCommentGroup.Use(jwt.JWTAuth(accountRepository, cache))
	protectedCommentGroup.POST("/publish", commentHandler.PublishComment)
	protectedCommentGroup.POST("/delete", commentHandler.DeleteComment)
	// social
	socialMQ, err := mq.NewSocialMQ(rmq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}
	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository, socialMQ, cache)
	socialHandler := social.NewSocialHandler(socialService)
	socialGroup := r.Group("/social")
	socialGroup.Use(jwt.JWTAuth(accountRepository, cache))
	socialGroup.POST("/follow", socialHandler.Follow)
	socialGroup.POST("/unfollow", socialHandler.Unfollow)
	socialGroup.POST("/getFollowers", socialHandler.GetFollowers)
	socialGroup.POST("/getFollowees", socialHandler.GetFollowees)
	socialGroup.POST("/getCounts", socialHandler.GetCounts)
	// feed
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)
	feedGroup := r.Group("/feed")
	feedGroup.POST("/latest", jwt.SoftJWTAuth(accountRepository, cache), feedHandler.ListLatest)
	feedGroup.POST("/listByPopularity", jwt.SoftJWTAuth(accountRepository, cache), feedHandler.ListByPopularity)
	feedGroup.POST("/following", jwt.JWTAuth(accountRepository, cache), feedHandler.ListFollowing)

	return r
}
