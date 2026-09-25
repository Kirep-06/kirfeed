package rediscache

import "fmt"

func AuthorOutboxKey(authorID uint) string {
	return fmt.Sprintf("feed:author_outbox:%d", authorID)
}

func FollowerInboxKey(followerID uint) string {
	return fmt.Sprintf("feed:follower_inbox:%d", followerID)
}
