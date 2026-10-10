package feed

import (
	"kirfeed/internal/apierror"
	"kirfeed/internal/infra/jwt"
	"net/http"

	"github.com/gin-gonic/gin"
)

type FeedHandler struct {
	service *FeedService
}

func NewFeedHandler(service *FeedService) *FeedHandler {
	return &FeedHandler{service: service}
}

const DefaultLimit = 20

func (f *FeedHandler) ListLatest(c *gin.Context) {
	var req ListLatestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrValidation), gin.H{"error": err.Error()})
		return
	}
	cursor, err := parseTimelineCursor(req.Cursor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = DefaultLimit
	}

	viewerID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerID = 0
	}
	feedItems, err := f.service.ListLatest(c.Request.Context(), req.Limit, cursor, viewerID)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	feedItems.VideoList = nonNilFeedVideoItems(feedItems.VideoList)
	c.JSON(http.StatusOK, feedItems)
}

func nonNilFeedVideoItems(items []FeedVideoItem) []FeedVideoItem {
	if items == nil {
		return []FeedVideoItem{}
	}
	return items
}

func (f *FeedHandler) ListFollowing(c *gin.Context) {
	var req ListFollowingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cursor, err := parseTimelineCursor(req.Cursor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = DefaultLimit
	}

	viewerID, err := jwt.GetAccountID(c)
	if err != nil || viewerID == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "login required"})
		return
	}
	resp, err := f.service.ListFollowing(c.Request.Context(), req.Limit, cursor, viewerID)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	resp.VideoList = nonNilFeedVideoItems(resp.VideoList)
	c.JSON(http.StatusOK, resp)
}

func (f *FeedHandler) ListByPopularity(c *gin.Context) {
	var req ListByPopularityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if req.Offset < 0 || req.AsOf < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}
	popularCursor, err := parsePopularCursor(req.Cursor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = DefaultLimit
	}

	viewerID, _ := jwt.GetAccountID(c)
	resp, err := f.service.ListByPopularity(
		c.Request.Context(),
		req.Limit,
		req.AsOf,
		req.Offset,
		viewerID,
		popularCursor,
	)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	resp.VideoList = nonNilFeedVideoItems(resp.VideoList)
	c.JSON(http.StatusOK, resp)
}
