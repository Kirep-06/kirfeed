package feed

import (
	"context"
	"kirfeed/internal/social"
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

func (repo *FeedRepository) ListFollowingRelations(ctx context.Context, followerID uint) ([]social.Social, error) {
	var relations []social.Social
	err := repo.db.WithContext(ctx).Where("follower_id = ?", followerID).Order("created_at ASC, id ASC").Find(&relations).Error
	return relations, err
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

// ListFollowingVideos reads the feed directly from MySQL when Redis is unavailable.
func (repo *FeedRepository) ListFollowingVideos(ctx context.Context, followerID uint, cursor TimelineCursor, limit int) ([]*video.Video, error) {
	var videos []*video.Video
	query := repo.db.WithContext(ctx).
		Table("videos AS v").
		Select("v.*").
		Joins("JOIN socials AS f ON f.followee_id = v.author_id").
		Where("f.follower_id = ?", followerID)
	if !cursor.CreateTime.IsZero() && cursor.VideoID > 0 {
		query = query.Where("v.create_time < ? OR (v.create_time = ? AND v.id < ?)", cursor.CreateTime, cursor.CreateTime, cursor.VideoID)
	}
	err := query.Order("v.create_time DESC, v.id DESC").Limit(limit).Scan(&videos).Error
	return videos, err
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
