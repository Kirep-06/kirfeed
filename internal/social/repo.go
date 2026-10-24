package social

import (
	"context"
	"errors"
	"kirfeed/internal/account"

	"gorm.io/gorm"
)

var (
	ErrAlreadyFollowed = errors.New("already followed")
	ErrNotFollowed     = errors.New("not followed")
)

type SocialRepository struct {
	db *gorm.DB
}

func NewSocialRepository(db *gorm.DB) *SocialRepository {
	return &SocialRepository{db: db}
}

func (r *SocialRepository) Follow(ctx context.Context, social *Social) (bool, error) {
	classificationChanged := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&Social{}).Where("follower_id = ? AND followee_id = ?", social.FollowerID, social.FolloweeID).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrAlreadyFollowed
		}
		var followersBefore int64
		if err := tx.Model(&Social{}).Where("followee_id = ?", social.FolloweeID).Count(&followersBefore).Error; err != nil {
			return err
		}
		if err := tx.Create(social).Error; err != nil {
			return err
		}
		classificationChanged = crossesCreatorBoundary(followersBefore, followersBefore+1)
		return nil
	})
	return classificationChanged, err
}

func (r *SocialRepository) Unfollow(ctx context.Context, social *Social) (bool, error) {
	classificationChanged := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var followersBefore int64
		if err := tx.Model(&Social{}).Where("followee_id = ?", social.FolloweeID).Count(&followersBefore).Error; err != nil {
			return err
		}
		result := tx.Where("follower_id = ? AND followee_id = ?", social.FollowerID, social.FolloweeID).Delete(&Social{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrNotFollowed
		}
		classificationChanged = crossesCreatorBoundary(followersBefore, followersBefore-1)
		return nil
	})
	return classificationChanged, err
}

func crossesCreatorBoundary(before, after int64) bool {
	threshold := int64(BigCreatorFollowerThreshold)
	return (before < threshold && after >= threshold) || (before >= threshold && after < threshold)
}

func (r *SocialRepository) GetFollowers(ctx context.Context, followeeID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	return r.listAccounts(ctx, "followee_id", followeeID, limit, cursor, true)
}

func (r *SocialRepository) RecentFollowerRelations(ctx context.Context, followeeID uint) ([]Social, error) {
	var relations []Social
	err := r.db.WithContext(ctx).Model(&Social{}).Select("id", "follower_id").Where("followee_id = ?", followeeID).Order("id DESC").Limit(101).Find(&relations).Error
	return relations, err
}

func (r *SocialRepository) FollowerAccountsByIDs(ctx context.Context, ids []uint) ([]*account.Account, error) {
	if len(ids) == 0 {
		return []*account.Account{}, nil
	}
	var accounts []*account.Account
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&accounts).Error; err != nil {
		return nil, err
	}
	byID := make(map[uint]*account.Account, len(accounts))
	for _, acc := range accounts {
		byID[acc.ID] = acc
	}
	ordered := make([]*account.Account, 0, len(ids))
	for _, id := range ids {
		if acc := byID[id]; acc != nil {
			ordered = append(ordered, acc)
		}
	}
	return ordered, nil
}

func (r *SocialRepository) GetFollowees(ctx context.Context, followerID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	return r.listAccounts(ctx, "follower_id", followerID, limit, cursor, false)
}

func (r *SocialRepository) FollowingRelations(ctx context.Context, followerID uint) ([]Social, error) {
	var relations []Social
	err := r.db.WithContext(ctx).Model(&Social{}).Where("follower_id = ?", followerID).Order("id DESC").Find(&relations).Error
	return relations, err
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
		Where("follower_id = ? AND followee_id = ?", social.FollowerID, social.FolloweeID).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (r *SocialRepository) CountFollowers(ctx context.Context, followeeID uint) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).Model(&Social{}).Where("followee_id = ?", followeeID).Count(&count).Error; err != nil {
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
	return r.listRelationIDs(ctx, "followee_id", followeeID, "follower_id", limit, cursor)
}

func (r *SocialRepository) ListFolloweeIDs(ctx context.Context, followerID uint, limit int, cursor uint) ([]uint, uint, bool, error) {
	return r.listRelationIDs(ctx, "follower_id", followerID, "followee_id", limit, cursor)
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
