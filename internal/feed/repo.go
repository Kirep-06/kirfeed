package feed

import (
	"context"
	"kirfeed/internal/video"
	"time"

	"gorm.io/gorm"
)

type FeedRepository struct {
	db *gorm.DB
}

func NewFeedRepository(db *gorm.DB) *FeedRepository {
	return &FeedRepository{db: db}
}

func (repo *FeedRepository) ListLatest(ctx context.Context, limit int, latestBefore time.Time) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC")
	if !latestBefore.IsZero() {
		query = query.Where("create_time < ?", latestBefore)
	}
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

func (repo *FeedRepository) GetByIDs(ctx context.Context, ids []uint) ([]*video.Video, error) {
	var videos []*video.Video
	if len(ids) == 0 {
		return videos, nil
	}
	if err := repo.db.WithContext(ctx).Model(&video.Video{}).
		Where("id IN ?", ids).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

// ListFollowingAuthors returns current followees and their current follower counts.
func (repo *FeedRepository) ListFollowingAuthors(ctx context.Context, followerID uint) ([]followingAuthor, error) {
	var authors []followingAuthor
	err := repo.db.WithContext(ctx).Table("socials AS s").
		Select("s.vlogger_id AS author_id, (SELECT COUNT(*) FROM socials AS f WHERE f.vlogger_id = s.vlogger_id) AS follower_count").
		Where("s.follower_id = ?", followerID).
		Scan(&authors).Error
	return authors, err
}

// ListFollowingVideos is the source of truth for a cold or incomplete Redis page.
func (repo *FeedRepository) ListFollowingVideos(ctx context.Context, followerID uint, cursor *followingCursor, limit int) ([]*video.Video, error) {
	var videos []*video.Video
	const timeScore = "FLOOR(UNIX_TIMESTAMP(v.create_time) * 1000)"
	query := repo.db.WithContext(ctx).Table("videos AS v").
		Select("v.*").
		Joins("JOIN socials AS s ON s.vlogger_id = v.author_id").
		Where("s.follower_id = ?", followerID)
	if cursor != nil {
		query = query.Where("("+timeScore+" < ? OR ("+timeScore+" = ? AND v.id < ?))", cursor.CreateTime.UnixMilli(), cursor.CreateTime.UnixMilli(), cursor.VideoID)
	}
	err := query.Order(timeScore + " DESC, v.id DESC").Limit(limit).Scan(&videos).Error
	return videos, err
}

func (repo *FeedRepository) ListByPopularity(ctx context.Context, limit int, popularityBefore int64, timeBefore time.Time, idBefore uint) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).Order("popularity DESC, create_time DESC, id DESC")
	if !timeBefore.IsZero() && idBefore > 0 {
		query = query.Where("(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)", popularityBefore, popularityBefore, timeBefore, popularityBefore, timeBefore, idBefore)
	}
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}
