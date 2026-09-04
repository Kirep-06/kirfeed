package feed

import (
	"kirfeed/internal/apierror"
	"kirfeed/internal/infra/jwt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type FeedHandler struct {
	service *FeedService
}

func NewFeedHandler(service *FeedService) *FeedHandler {
	return &FeedHandler{service: service}
}

func (f *FeedHandler) ListLatest(c *gin.Context) {
	var req ListLatestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(apierror.ErrValidation), gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.UnixMilli(req.LatestTime)
	}
	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}
	feedItems, err := f.service.ListLatest(c.Request.Context(), req.Limit, latestTime, viewerAccountID)
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

func (f *FeedHandler) ListByPopularity(c *gin.Context) {
	var req ListByPopularityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	if req.Offset < 0 || req.AsOf < 0 || req.LatestPopularity < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid input"})
		return
	}
	var latestID uint
	if !req.LatestBefore.IsZero() || req.LatestIDBefore != nil {
		if req.LatestBefore.IsZero() || req.LatestIDBefore == nil || *req.LatestIDBefore == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "latest_before and latest_id_before must be provided together"})
			return
		}
		latestID = *req.LatestIDBefore
	}
	viewerID, _ := jwt.GetAccountID(c)
	resp, err := f.service.ListByPopularity(
		c.Request.Context(),
		req.Limit,
		req.AsOf,
		req.Offset,
		viewerID,
		req.LatestPopularity,
		req.LatestBefore,
		latestID,
	)
	if err != nil {
		c.JSON(apierror.ClassifyHTTPStatus(err), gin.H{"error": err.Error()})
		return
	}
	resp.VideoList = nonNilFeedVideoItems(resp.VideoList)
	c.JSON(http.StatusOK, resp)
}
