package social

import (
	"context"
	"kirfeed/internal/account"

	"gorm.io/gorm"
)

type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

func (r *SocialRepository) Follow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).Create(social).Error
}

func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.FolloweeID).
		Delete(&Social{}).Error
}

func (r *SocialRepository) GetFollowers(ctx context.Context, followeeID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	return r.listAccounts(ctx, "vlogger_id", followeeID, limit, cursor, true)
}

func (r *SocialRepository) GetFollowees(ctx context.Context, followerID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	return r.listAccounts(ctx, "follower_id", followerID, limit, cursor, false)
}

func (r *SocialRepository) listAccounts(ctx context.Context, relationColumn string, ownerID uint, limit int, cursor uint, followers bool) ([]*account.Account, uint, bool, error) {
	var relations []Social
	query := r.db.WithContext(ctx).Model(&Social{}).Where(relationColumn+" = ?", ownerID).Order("id DESC")
	if cursor != 0 {
		query = query.Where("id < ?", cursor)
	}
	if err := query.Limit(limit + 1).Find(&relations).Error; err != nil {
		return nil, 0, false, err
	}
	hasMore := len(relations) > limit
	if hasMore {
		relations = relations[:limit]
	}
	if len(relations) == 0 {
		return []*account.Account{}, 0, false, nil
	}
	ids := make([]uint, 0, len(relations))
	for _, rel := range relations {
		if followers {
			ids = append(ids, rel.FollowerID)
		} else {
			ids = append(ids, rel.FolloweeID)
		}
	}
	var accounts []*account.Account
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&accounts).Error; err != nil {
		return nil, 0, false, err
	}
	byID := make(map[uint]*account.Account, len(accounts))
	for _, acc := range accounts {
		byID[acc.ID] = acc
	}
	ordered := make([]*account.Account, 0, len(relations))
	for _, id := range ids {
		if acc := byID[id]; acc != nil {
			ordered = append(ordered, acc)
		}
	}
	var nextCursor uint
	if hasMore {
		nextCursor = relations[len(relations)-1].ID
	}
	return ordered, nextCursor, hasMore, nil
}

func (r *SocialRepository) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&Social{}).
		Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.FolloweeID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SocialRepository) CountFollowers(ctx context.Context, followeeID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("vlogger_id = ?", followeeID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *SocialRepository) CountFollowees(ctx context.Context, followerID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("follower_id = ?", followerID).Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (r *SocialRepository) ListFollowerIDs(ctx context.Context, followeeID uint, limit int, cursor uint) ([]uint, uint, bool, error) {
	return r.listRelationIDs(ctx, "vlogger_id", followeeID, "follower_id", limit, cursor)
}

func (r *SocialRepository) ListFolloweeIDs(ctx context.Context, followerID uint, limit int, cursor uint) ([]uint, uint, bool, error) {
	return r.listRelationIDs(ctx, "follower_id", followerID, "vlogger_id", limit, cursor)
}

func (r *SocialRepository) listRelationIDs(ctx context.Context, ownerColumn string, ownerID uint, targetColumn string, limit int, cursor uint) ([]uint, uint, bool, error) {
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	var relations []Social
	query := r.db.WithContext(ctx).Model(&Social{}).
		Select("id", targetColumn).
		Where(ownerColumn+" = ?", ownerID).
		Order("id DESC")
	if cursor != 0 {
		query = query.Where("id < ?", cursor)
	}
	if err := query.Limit(limit + 1).Find(&relations).Error; err != nil {
		return nil, 0, false, err
	}
	hasMore := len(relations) > limit
	if hasMore {
		relations = relations[:limit]
	}
	ids := make([]uint, 0, len(relations))
	for _, relation := range relations {
		if targetColumn == "follower_id" {
			ids = append(ids, relation.FollowerID)
		} else {
			ids = append(ids, relation.FolloweeID)
		}
	}
	var nextCursor uint
	if hasMore {
		nextCursor = relations[len(relations)-1].ID
	}
	return ids, nextCursor, hasMore, nil
}
