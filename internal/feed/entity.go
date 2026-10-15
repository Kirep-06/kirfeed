package feed

import "time"

type FeedAuthor struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

type FeedVideoItem struct {
	ID          uint       `json:"id"`
	Author      FeedAuthor `json:"author"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	PlayURL     string     `json:"play_url"`
	CoverURL    string     `json:"cover_url"`
	CreateTime  int64      `json:"create_time"`
	LikesCount  int64      `json:"likes_count"`
	IsLiked     bool       `json:"is_liked"`
}

type ListLatestRequest struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

type ListLatestResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	NextCursor string          `json:"next_cursor"`
	HasMore    bool            `json:"has_more"`
}

type ListFollowingRequest struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

type ListFollowingResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	NextCursor string          `json:"next_cursor"`
	HasMore    bool            `json:"has_more"`
}

type ListByPopularityRequest struct {
	Limit  int    `json:"limit"`
	AsOf   int64  `json:"as_of"`  // 服务器返回的分钟时间戳；第一页传0
	Offset int    `json:"offset"` // 下一页从这里开始；第一页传0
	Cursor string `json:"cursor"` // MySQL 兜底分页游标
}

type ListPopularityResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	AsOf       int64           `json:"as_of"`
	NextOffset int             `json:"next_offset"`
	HasMore    bool            `json:"has_more"`
	NextCursor string          `json:"next_cursor"`
}

type TimelineCursor struct {
	CreateTime time.Time
	VideoID    uint
}

type PopularCursor struct {
	Popularity int64     `json:"popularity"`
	CreateTime time.Time `json:"create_time"`
	VideoID    uint      `json:"video_id"`
}
