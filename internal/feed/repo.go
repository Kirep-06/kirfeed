package feed

import (
	"context"
	"kirfeed/internal/video"

	"gorm.io/gorm"
)

type FeedRepository struct {
	db *gorm.DB
}

func NewFeedRepository(db *gorm.DB) *FeedRepository {
	return &FeedRepository{db: db}
}

func (repo *FeedRepository) ListLatest(ctx context.Context, limit int, cursor TimelineCursor) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).
		Order("create_time DESC, id DESC")
	if !cursor.CreateTime.IsZero() && cursor.VideoID > 0 {
		query = query.Where("create_time < ? OR (create_time = ? AND id < ?)", cursor.CreateTime, cursor.CreateTime, cursor.VideoID)
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

func (repo *FeedRepository) ListFollowingAuthorIDs(ctx context.Context, followerID uint) ([]uint, error) {
	var ids []uint
	err := repo.db.WithContext(ctx).Table("socials").Where("follower_id = ?", followerID).Pluck("followee_id", &ids).Error
	return ids, err
}

func (repo *FeedRepository) ListBigFollowingAuthorIDs(ctx context.Context, authorIDs []uint) ([]uint, error) {
	var ids []uint
	if len(authorIDs) == 0 {
		return ids, nil
	}
	err := repo.db.WithContext(ctx).Table("socials").
		Select("followee_id").
		Where("followee_id IN ?", authorIDs).
		Group("followee_id").
		Having("COUNT(*) >= ?", BigCreatorFollowerThreshold).
		Pluck("followee_id", &ids).Error
	return ids, err
}

func (repo *FeedRepository) ListByPopularity(ctx context.Context, limit int, cursor PopularCursor) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).Model(&video.Video{}).Order("popularity DESC, create_time DESC, id DESC")
	if !cursor.CreateTime.IsZero() && cursor.VideoID > 0 {
		query = query.Where(
			"(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)",
			cursor.Popularity, cursor.Popularity, cursor.CreateTime, cursor.Popularity, cursor.CreateTime, cursor.VideoID,
		)
	}
	if err := query.Limit(limit).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}
