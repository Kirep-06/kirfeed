package social

import (
	"time"

	"kirfeed/internal/account"
)

const BigCreatorFollowerThreshold = 1000

type Social struct {
	ID         uint      `gorm:"primaryKey"`
	FollowerID uint      `gorm:"not null;index:idx_social_follower;uniqueIndex:idx_social_follower_vlogger"`
	FolloweeID uint      `gorm:"not null;index:idx_social_vlogger;uniqueIndex:idx_social_follower_vlogger"`
	CreatedAt  time.Time `gorm:"not null;autoCreateTime:milli;default:CURRENT_TIMESTAMP(3)"`
}

type FollowRequest struct {
	FolloweeID uint `json:"followee_id"`
}

type UnfollowRequest struct {
	FolloweeID uint `json:"followee_id"`
}

type GetFollowersRequest struct {
	FolloweeID uint `json:"followee_id"`
	Limit      int  `json:"limit"`
	Cursor     uint `json:"cursor"`
}

type GetFollowersResponse struct {
	Followers     []*account.Account `json:"followers"`
	FollowerCount int64              `json:"follower_count"`
	NextCursor    uint               `json:"next_cursor"`
	HasMore       bool               `json:"has_more"`
}

type GetFolloweesResponse struct {
	Followees     []*account.Account `json:"followees"`
	FolloweeCount int64              `json:"followee_count"`
	NextCursor    uint               `json:"next_cursor"`
	HasMore       bool               `json:"has_more"`
}

type SocialCounts struct {
	FollowerCount int64 `json:"follower_count"`
	FolloweeCount int64 `json:"followee_count"`
}

type GetFolloweesRequest struct {
	FollowerID uint `json:"follower_id"`
	Limit      int  `json:"limit"`
	Cursor     uint `json:"cursor"`
}
