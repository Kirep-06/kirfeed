package rediscache

import (
	"time"
)

func (c *Client) AccountAccessTokenKey(accountID uint) string {
	return c.Key("account:%d", accountID)
}

func (c *Client) AccountRefreshTokenKey(accountID uint) string {
	return c.Key("account:%d:refresh", accountID)
}

func (c *Client) RefreshTokenAccountKey(refreshToken string) string {
	return c.Key("refresh:%s", refreshToken)
}

func (c *Client) VideoEntityKey(videoID uint) string {
	return c.Key("video:entity:%d", videoID)
}

func (c *Client) VideoDetailKey(videoID uint) string {
	return c.Key("video:detail:id=%d", videoID)
}

func (c *Client) LockKey(key string) string {
	return "lock:" + key
}

func (c *Client) VideoEntitySingleflightKey(videoID uint) string {
	return c.Key("sf:entity:%d", videoID)
}

func (c *Client) GlobalTimelineKey() string {
	return c.Key("feed:global_timeline")
}

func (c *Client) GlobalTimelineRebuildSingleflightKey() string {
	return c.Key("sf:fallback:global_timeline_rebuild")
}

func (c *Client) ColdLatestSingleflightKey(limit int, reqTime int64) string {
	return c.Key("sf:cold:listLatest:%d:%d", limit, reqTime)
}

func (c *Client) StitchedLatestSingleflightKey(limit int, cursorTime int64) string {
	return c.Key("sf:stitch:listLatest:%d:%d", limit, cursorTime)
}

func (c *Client) HotVideoMinuteKey(minute time.Time) string {
	return c.Key("hot:video:1m:%s", minute.Format("200601021504"))
}

func (c *Client) MergedHotVideoMinuteKey(minute time.Time) string {
	return c.Key("hot:video:merge:1m:%s", minute.Format("200601021504"))
}

func (c *Client) FollowingFeedPatternKey(accountID uint) string {
	return c.Key("feed:listByFollowing:*:accountID=%d:*", accountID)
}

func (c *Client) FollowingRelationsKey(accountID uint) string {
	return c.Key("feed:listByFollowing:relations:accountID=%d:meta", accountID)
}

func (c *Client) AuthorOutboxKey(authorID uint) string {
	return c.Key("feed:author_outbox:%d", authorID)
}

func (c *Client) FollowerInboxKey(followerID uint) string {
	return c.Key("feed:follower_inbox:%d", followerID)
}
