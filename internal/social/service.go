package social

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"kirfeed/internal/account"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"

	"log"
)

type SocialService struct {
	repo        *SocialRepository
	accountrepo *account.AccountRepository
	socialMQ    *mq.SocialMQ
	cache       *rediscache.Client
}

const socialCacheTTL = 15 * time.Minute

func NewSocialService(repo *SocialRepository, accountrepo *account.AccountRepository, socialMQ *mq.SocialMQ, cache *rediscache.Client) *SocialService {
	return &SocialService{repo: repo, accountrepo: accountrepo, socialMQ: socialMQ, cache: cache}
}

func (s *SocialService) Follow(ctx context.Context, social *Social) error {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountrepo.FindByID(ctx, social.FolloweeID)
	if err != nil {
		return err
	}
	if social.FollowerID == social.FolloweeID {
		return errors.New("can not follow self")
	}
	// 先写 DB，确保数据持久化
	classificationChanged, err := s.repo.Follow(ctx, social)
	if err != nil {
		if errors.Is(err, ErrAlreadyFollowed) {
			return errors.New("already followed")
		}
		return err
	}

	s.invalidateRelationshipCaches(context.Background(), social.FollowerID, social.FolloweeID)
	if classificationChanged {
		if err := s.setAuthorClassification(context.Background(), social.FolloweeID, true); err != nil {
			log.Printf("更新大 V 分类缓存失败: authorID=%d, err=%v", social.FolloweeID, err)
			s.forgetAuthorClassification(context.Background(), social.FolloweeID)
		}
	}

	// 最后发 MQ（用于通知），失败只记日志不影响业务
	if s.socialMQ != nil {
		if err := s.socialMQ.Follow(ctx, social.FollowerID, social.FolloweeID); err != nil {
			log.Printf("social MQ Follow 发布失败: %v", err)
		}
	}
	return nil
}

func (s *SocialService) Unfollow(ctx context.Context, social *Social) error {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountrepo.FindByID(ctx, social.FolloweeID)
	if err != nil {
		return err
	}
	// 先写 DB
	classificationChanged, err := s.repo.Unfollow(ctx, social)
	if err != nil {
		if errors.Is(err, ErrNotFollowed) {
			return errors.New("not followed")
		}
		return err
	}

	s.invalidateRelationshipCaches(context.Background(), social.FollowerID, social.FolloweeID)
	if classificationChanged {
		if err := s.setAuthorClassification(context.Background(), social.FolloweeID, false); err != nil {
			log.Printf("更新大 V 分类缓存失败: authorID=%d, err=%v", social.FolloweeID, err)
			s.forgetAuthorClassification(context.Background(), social.FolloweeID)
		}
	}

	// 最后发 MQ
	if s.socialMQ != nil {
		if err := s.socialMQ.UnFollow(ctx, social.FollowerID, social.FolloweeID); err != nil {
			log.Printf("social MQ UnFollow 发布失败: %v", err)
		}
	}
	return nil
}

func (s *SocialService) invalidateRelationshipCaches(ctx context.Context, followerID, followeeID uint) {
	if s.cache == nil {
		return
	}
	if err := s.cache.InvalidateSocialRelationship(ctx, followerID, followeeID); err != nil {
		log.Printf("失效关注关系缓存失败: followerID=%d, followeeID=%d, err=%v", followerID, followeeID, err)
	}
}

func (s *SocialService) setAuthorClassification(ctx context.Context, authorID uint, isBig bool) error {
	if s.cache == nil {
		return nil
	}
	value := "0"
	if isBig {
		value = "1"
	}
	return s.cache.SetBytes(ctx, s.cache.AuthorClassificationKey(authorID), []byte(value), socialCacheTTL)
}

func (s *SocialService) forgetAuthorClassification(ctx context.Context, authorID uint) {
	if s.cache == nil {
		return
	}
	if err := s.cache.Del(ctx, s.cache.AuthorClassificationKey(authorID)); err != nil {
		log.Printf("清除大 V 分类缓存失败: authorID=%d, err=%v", authorID, err)
	}
}

func (s *SocialService) GetFollowers(ctx context.Context, FolloweeID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	_, err := s.accountrepo.FindByID(ctx, FolloweeID)
	if err != nil {
		return nil, 0, false, err
	}
	if s.cache != nil && limit > 0 && limit <= 100 {
		if followers, next, more, hit, err := s.cachedFollowers(ctx, FolloweeID, limit, cursor); err == nil && hit {
			return followers, next, more, nil
		}
	}
	return s.repo.GetFollowers(ctx, FolloweeID, limit, cursor)
}

func (s *SocialService) cachedFollowers(ctx context.Context, followeeID uint, limit int, cursor uint) ([]*account.Account, uint, bool, bool, error) {
	key := s.cache.RecentFollowersKey(followeeID)
	members, count, hit, err := s.cache.ReadZSetIndex(ctx, key, 100)
	if err != nil {
		return nil, 0, false, false, err
	}
	if !hit {
		generation, generationErr := s.cache.IndexGeneration(ctx, s.cache.RecentFollowersGenerationKey(followeeID))
		if generationErr != nil {
			return nil, 0, false, false, generationErr
		}
		relations, dbErr := s.repo.RecentFollowerRelations(ctx, followeeID)
		if dbErr != nil {
			return nil, 0, false, false, dbErr
		}
		members := make([]redis.Z, 0, min(len(relations), 100))
		for _, relation := range relations {
			if len(members) == 100 {
				break
			}
			members = append(members, redis.Z{Score: float64(relation.ID), Member: fmt.Sprintf("%d:%d", relation.ID, relation.FollowerID)})
		}
		stored, err := s.cache.StoreZSetIfGeneration(ctx, s.cache.RecentFollowersGenerationKey(followeeID), key, generation, members, len(relations), socialCacheTTL)
		if err != nil || !stored {
			return nil, 0, false, false, err
		}
		members, count, hit, err = s.cache.ReadZSetIndex(ctx, key, 100)
		if err != nil || !hit {
			return nil, 0, false, false, err
		}
	}
	if err != nil || count < 0 || count > 101 {
		return nil, 0, false, false, fmt.Errorf("invalid recent followers marker")
	}
	type relationID struct{ id, followerID uint }
	selected := make([]relationID, 0, limit+1)
	for i := len(members) - 1; i >= 0; i-- {
		var id, followerID uint
		if _, err := fmt.Sscanf(fmt.Sprint(members[i].Member), "%d:%d", &id, &followerID); err != nil {
			return nil, 0, false, false, err
		}
		if cursor != 0 && id >= cursor {
			continue
		}
		selected = append(selected, relationID{id, followerID})
		if len(selected) > limit {
			break
		}
	}
	if len(selected) == 0 && cursor != 0 && count == 101 {
		return nil, 0, false, false, nil
	}
	more := len(selected) > limit
	if more {
		selected = selected[:limit]
	}
	if !more && count == 101 && len(selected) > 0 && selected[len(selected)-1].id == uint(members[0].Score) {
		more = true
	}
	if !more && count == 101 && len(selected) < limit {
		return nil, 0, false, false, nil
	}
	ids := make([]uint, len(selected))
	for i, relation := range selected {
		ids[i] = relation.followerID
	}
	accounts, err := s.repo.FollowerAccountsByIDs(ctx, ids)
	if err != nil {
		return nil, 0, false, false, err
	}
	var next uint
	if more && len(selected) > 0 {
		next = selected[len(selected)-1].id
	}
	return accounts, next, more, true, nil
}

func (s *SocialService) GetFollowees(ctx context.Context, FollowerID uint, limit int, cursor uint) ([]*account.Account, uint, bool, error) {
	_, err := s.accountrepo.FindByID(ctx, FollowerID)
	if err != nil {
		return nil, 0, false, err
	}
	if s.cache != nil && limit > 0 && limit <= 100 {
		if accounts, next, more, hit, err := s.cachedFollowees(ctx, FollowerID, limit, cursor); err == nil && hit {
			return accounts, next, more, nil
		}
	}
	return s.repo.GetFollowees(ctx, FollowerID, limit, cursor)
}

func (s *SocialService) cachedFollowees(ctx context.Context, followerID uint, limit int, cursor uint) ([]*account.Account, uint, bool, bool, error) {
	key := s.cache.FollowingRelationsKey(followerID)
	members, _, hit, err := s.cache.ReadZSetIndex(ctx, key, 0)
	if err != nil {
		return nil, 0, false, false, err
	}
	if !hit {
		generation, genErr := s.cache.IndexGeneration(ctx, s.cache.FollowingRelationsGenerationKey(followerID))
		if genErr != nil {
			return nil, 0, false, false, genErr
		}
		relations, dbErr := s.repo.FollowingRelations(ctx, followerID)
		if dbErr != nil {
			return nil, 0, false, false, dbErr
		}
		members := make([]redis.Z, 0, len(relations))
		for _, relation := range relations {
			followedAt := relation.CreatedAt
			if followedAt.IsZero() {
				followedAt = time.UnixMilli(int64(relation.ID))
			}
			members = append(members, redis.Z{Score: float64(followedAt.UnixMilli()), Member: fmt.Sprintf("%020d:%d", relation.ID, relation.FolloweeID)})
		}
		stored, storeErr := s.cache.StoreZSetIfGeneration(ctx, s.cache.FollowingRelationsGenerationKey(followerID), key, generation, members, len(members), socialCacheTTL)
		if storeErr != nil || !stored {
			return nil, 0, false, false, storeErr
		}
		members, _, hit, err = s.cache.ReadZSetIndex(ctx, key, 0)
		if err != nil || !hit {
			return nil, 0, false, false, err
		}
	}
	selected, more, err := pageFollowingMembers(members, limit, cursor)
	if err != nil {
		return nil, 0, false, false, err
	}
	ids := make([]uint, len(selected))
	for i, item := range selected {
		ids[i] = item.authorID
	}
	accounts, err := s.repo.FollowerAccountsByIDs(ctx, ids)
	if err != nil {
		return nil, 0, false, false, err
	}
	var next uint
	if more && len(selected) > 0 {
		next = selected[len(selected)-1].id
	}
	return accounts, next, more, true, nil
}

type followingMember struct{ id, authorID uint }

func pageFollowingMembers(members []redis.Z, limit int, cursor uint) ([]followingMember, bool, error) {
	ordered := make([]followingMember, 0, len(members))
	for _, member := range members {
		var id, authorID uint
		if n, err := fmt.Sscanf(fmt.Sprint(member.Member), "%d:%d", &id, &authorID); err != nil || n != 2 || id == 0 || authorID == 0 {
			return nil, false, fmt.Errorf("invalid following member")
		}
		ordered = append(ordered, followingMember{id, authorID})
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].id > ordered[j].id })
	selected := make([]followingMember, 0, limit+1)
	for _, item := range ordered {
		if cursor != 0 && item.id >= cursor {
			continue
		}
		selected = append(selected, item)
		if len(selected) > limit {
			break
		}
	}
	more := len(selected) > limit
	if more {
		selected = selected[:limit]
	}
	return selected, more, nil
}

func (s *SocialService) CountFollowers(ctx context.Context, followeeID uint) (int64, error) {
	if s.cache == nil {
		return s.repo.CountFollowers(ctx, followeeID)
	}
	key, generationKey := s.cache.FollowersCountKey(followeeID), s.cache.FollowersCountGenerationKey(followeeID)
	return s.cachedCount(ctx, key, generationKey, func() (int64, error) { return s.repo.CountFollowers(ctx, followeeID) })
}

func (s *SocialService) CountFollowees(ctx context.Context, followerID uint) (int64, error) {
	if s.cache == nil {
		return s.repo.CountFollowees(ctx, followerID)
	}
	key, generationKey := s.cache.FollowingCountKey(followerID), s.cache.FollowingCountGenerationKey(followerID)
	return s.cachedCount(ctx, key, generationKey, func() (int64, error) { return s.repo.CountFollowees(ctx, followerID) })
}

func (s *SocialService) cachedCount(ctx context.Context, key, generationKey string, query func() (int64, error)) (int64, error) {
	if count, hit, err := s.cache.ReadCount(ctx, key); err == nil && hit {
		return count, nil
	}
	generation, generationErr := s.cache.IndexGeneration(ctx, generationKey)
	count, err := query()
	if err != nil {
		return 0, err
	}
	if generationErr == nil {
		_, _ = s.cache.StoreCountIfGeneration(ctx, generationKey, key, generation, count, socialCacheTTL)
	}
	return count, nil
}

func (s *SocialService) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return false, err
	}
	_, err = s.accountrepo.FindByID(ctx, social.FolloweeID)
	if err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, social)
}
