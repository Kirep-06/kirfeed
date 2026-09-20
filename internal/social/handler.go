package social

import (
	"kirfeed/internal/account"
	"kirfeed/internal/apierror"
	"kirfeed/internal/infra/jwt"

	"net/http"

	"github.com/gin-gonic/gin"
)

type SocialHandler struct {
	service *SocialService
}

func NewSocialHandler(service *SocialService) *SocialHandler {
	return &SocialHandler{service: service}
}

func (h *SocialHandler) Follow(c *gin.Context) {
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if req.FolloweeID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "followee_id is required"})
		return
	}
	FollowerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	social := &Social{
		FollowerID: FollowerID,
		FolloweeID: req.FolloweeID,
	}
	if err := h.service.Follow(c.Request.Context(), social); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "followed"})
}

func (h *SocialHandler) Unfollow(c *gin.Context) {
	var req UnfollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if req.FolloweeID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "followee_id is required"})
		return
	}
	FollowerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	social := &Social{
		FollowerID: FollowerID,
		FolloweeID: req.FolloweeID,
	}
	if err := h.service.Unfollow(c.Request.Context(), social); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "unfollowed"})
}

func (h *SocialHandler) GetFollowers(c *gin.Context) {
	var req GetFollowersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}

	followeeID := req.FolloweeID
	if followeeID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		followeeID = accountID
	}

	if req.Limit <= 0 || req.Limit > 100 {
		req.Limit = 20
	}
	followers, nextCursor, hasMore, err := h.service.GetFollowers(c.Request.Context(), followeeID, req.Limit, req.Cursor)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if followers == nil {
		followers = []*account.Account{}
	}
	followerCount, _ := h.service.CountFollowers(c.Request.Context(), followeeID)
	c.JSON(http.StatusOK, GetFollowersResponse{Followers: followers, FollowerCount: followerCount, NextCursor: nextCursor, HasMore: hasMore})
}

func (h *SocialHandler) GetFollowees(c *gin.Context) {
	var req GetFolloweesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}

	followerID := req.FollowerID
	if followerID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		followerID = accountID
	}

	if req.Limit <= 0 || req.Limit > 100 {
		req.Limit = 20
	}
	followees, nextCursor, hasMore, err := h.service.GetFollowees(c.Request.Context(), followerID, req.Limit, req.Cursor)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if followees == nil {
		followees = []*account.Account{}
	}
	followeeCount, _ := h.service.CountFollowees(c.Request.Context(), followerID)
	c.JSON(http.StatusOK, GetFolloweesResponse{Followees: followees, FolloweeCount: followeeCount, NextCursor: nextCursor, HasMore: hasMore})
}

func (h *SocialHandler) GetCounts(c *gin.Context) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	followerCount, _ := h.service.CountFollowers(c.Request.Context(), accountID)
	followeeCount, _ := h.service.CountFollowees(c.Request.Context(), accountID)
	c.JSON(http.StatusOK, SocialCounts{FollowerCount: followerCount, FolloweeCount: followeeCount})
}
