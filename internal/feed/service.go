package feed

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/video"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"

	cache "github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
)

type FeedService struct {
	repo         *FeedRepository
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client
	localcache   *cache.Cache
	cacheTTL     time.Duration
	requestGroup singleflight.Group
}

type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client) *FeedService {
	return &FeedService{repo: repo, likeRepo: likeRepo, rediscache: rediscache, localcache: cache.New(3*time.Second, 5*time.Second), cacheTTL: 24 * time.Hour}
}

func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	// GetVideoByIDs 批量获取视频信息
	// 采用 L1(本地缓存) -> L2(Redis) -> L3(MySQL) 三级架构
	if len(videoIDs) == 0 {
		return []*video.Video{}, nil
	}

	videoMap := make(map[uint]*video.Video)
	//L1:本地缓存
	var missedL1 []uint
	for _, id := range videoIDs {
		cacheKey := f.rediscache.VideoEntityKey(id)
		if f.localcache != nil {
			if v, found := f.localcache.Get(cacheKey); found {
				if data, ok := v.(video.Video); ok {
					videoMap[id] = &data
					continue
				}
			}
		}
		// 记录未命中的 ID，准备进入下一级缓存
		missedL1 = append(missedL1, id)
	}

	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	//L2:redis
	var missedL2 []uint
	if len(missedL1) > 0 {
		cacheKeys := make([]string, len(missedL1))
		for i, id := range missedL1 {
			cacheKeys[i] = f.rediscache.VideoEntityKey(id)
		}

		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
		cancel()

		if err == nil {
			for i, res := range results {
				id := missedL1[i]
				if res != nil {
					if str, ok := res.(string); ok {
						var v video.Video
						if err := json.Unmarshal([]byte(str), &v); err == nil {
							videoMap[id] = &v
							// 回写更新 L1 本地缓存
							if f.localcache != nil {
								f.localcache.Set(cacheKeys[i], v, 5*time.Second)
							}
							continue
						}
					}
				}
				missedL2 = append(missedL2, id)
			}
		} else {
			// 如果 Redis 挂了或者超时了，全部降级到 L3
			missedL2 = missedL1
			log.Printf("L2 Redis MGet 失败，全部降级到 MySQL: %v", err)
		}
	}

	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// L3: 批量回源 MySQL
	videos, err := f.repo.GetByIDs(ctx, missedL2)
	if err != nil {
		return nil, err
	}
	var pipe redis.Pipeliner
	if f.rediscache != nil {
		pipe = f.rediscache.Pipeline()
	}
	for _, v := range videos {
		safeCopy := *v
		videoMap[v.ID] = &safeCopy
		cacheKey := f.rediscache.VideoEntityKey(v.ID)
		if f.localcache != nil {
			f.localcache.Set(cacheKey, safeCopy, 5*time.Second)
		}
		if pipe != nil {
			if b, err := json.Marshal(safeCopy); err == nil {
				pipe.Set(ctx, cacheKey, b, time.Hour)
			}
		}
	}
	if pipe != nil {
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_, _ = pipe.Exec(cacheCtx)
		cancel()
	}
	return buildOrderedResult(videoIDs, videoMap), nil
}

// 查询最新视频 (冷热分离 + 游标分页)
func (f *FeedService) ListLatest(ctx context.Context, limit int, cursor TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {
	if f.rediscache == nil {
		log.Printf("latest feed Redis client unavailable, falling back to MySQL")
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	// 获取 ZSET 中最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, f.rediscache.GlobalTimelineKey(), 0, 0)
	if err != nil {
		log.Printf("latest feed Redis tail read failed, falling back to MySQL: %v", err)
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	isZsetEmpty := len(zsetTail) == 0

	if isZsetEmpty {
		sfKey := f.rediscache.GlobalTimelineKey()

		_, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			// 无视游标，直接去 MySQL 捞最新的 1000 条
			dbVideos, err := f.repo.ListLatest(ctx, 1000, TimelineCursor{})
			if err != nil {
				return nil, err
			}
			if len(dbVideos) == 0 {
				return nil, nil
			}

			// 重建 ZSET
			bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var zElements []redis.Z
			for _, vid := range dbVideos {
				zElements = append(zElements, redis.Z{
					Score:  float64(vid.CreateTime.UnixMilli()),
					Member: fmt.Sprintf("%d", vid.ID),
				})
			}
			if err := f.rediscache.ZAdd(bgCtx, f.rediscache.GlobalTimelineKey(), zElements...); err != nil {
				return nil, err
			}
			return "SUCCESS", nil
		})

		if err != nil {
			log.Printf("latest feed index rebuild failed, falling back to MySQL: %v", err)
		} else {
			log.Printf("latest feed Redis index was empty, reading page from MySQL")
		}
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	watermark := int64(zsetTail[0].Score)
	reqTime := time.Now().UnixMilli()
	if !cursor.CreateTime.IsZero() {
		reqTime = cursor.CreateTime.UnixMilli()
	}

	if reqTime <= watermark {
		log.Printf("latest feed cursor reached Redis watermark, falling back to MySQL")
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}
	videoIDs, err := f.latestRedisIDs(ctx, limit, cursor)
	if err != nil {
		log.Printf("latest feed Redis read failed, falling back to MySQL: %v", err)
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}
	baseVideos, err := f.GetVideoByIDs(ctx, videoIDs)
	if err != nil {
		log.Printf("latest feed video lookup failed, falling back to MySQL: %v", err)
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}
	sortLatestVideos(baseVideos)
	// 刚好击穿了冷热边界
	if len(baseVideos) < limit+1 {
		log.Printf("latest feed Redis page incomplete, filling from MySQL")
		remainLimit := limit - len(baseVideos) // 计算还差几个

		var coldCursor TimelineCursor
		if len(baseVideos) > 0 {
			last := baseVideos[len(baseVideos)-1]
			coldCursor = TimelineCursor{
				CreateTime: last.CreateTime,
				VideoID:    last.ID,
			}
		} else {
			coldCursor = cursor
		}

		sfKey := f.rediscache.GlobalTimelineKey()
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			return f.repo.ListLatest(ctx, remainLimit+1, coldCursor)
		})

		if err != nil {
			return ListLatestResponse{}, err
		}

		coldVideos := v.([]*video.Video)
		baseVideos = append(baseVideos, coldVideos...)

		sortLatestVideos(baseVideos)
	}
	return f.timelineResponse(ctx, baseVideos, limit, viewerAccountID)
}

func (f *FeedService) latestRedisIDs(ctx context.Context, limit int, cursor TimelineCursor) ([]uint, error) {
	key := f.rediscache.GlobalTimelineKey()
	ids := make([]uint, 0, limit+1)

	// 如果不是首页,取当前 curScore && id<cursor.id
	if !cursor.CreateTime.IsZero() {
		curScore := fmt.Sprint(cursor.CreateTime.UnixMilli())
		sameTime, err := f.rediscache.ZRevRangeByScore(
			ctx,
			key,
			curScore,
			curScore,
			0,
			1000,
		)
		if err != nil {
			return nil, err
		}
		for _, member := range sameTime {
			id, err := strconv.ParseUint(member, 10, 64)
			if err == nil && id > 0 && uint(id) < cursor.VideoID {
				ids = append(ids, uint(id))
			}
		}
		if len(ids) >= limit+1 {
			return ids, nil
		}
	}

	// 取 curScore < time
	maxScore := "+inf"
	if !cursor.CreateTime.IsZero() {
		maxScore = fmt.Sprint(cursor.CreateTime.UnixMilli() - 1)
	}
	earlier, err := f.rediscache.ZRevRangeByScore(
		ctx,
		key,
		maxScore,
		"-inf",
		0,
		int64(limit-len(ids)+1),
	)
	if err != nil {
		return nil, err
	}

	// 组装
	for _, member := range earlier {
		id, err := strconv.ParseUint(member, 10, 64)
		if err == nil && id > 0 {
			ids = append(ids, uint(id))
		}
	}
	return ids, nil
}

func (f *FeedService) listLatestFromDB(ctx context.Context, limit int, cursor TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {
	videos, err := f.repo.ListLatest(ctx, limit+1, cursor)
	if err != nil {
		return ListLatestResponse{}, err
	}
	return f.timelineResponse(ctx, videos, limit, viewerAccountID)
}

func sortLatestVideos(videos []*video.Video) {
	sort.Slice(videos, func(i, j int) bool {
		if videos[i].CreateTime.UnixMilli() == videos[j].CreateTime.UnixMilli() {
			return videos[i].ID > videos[j].ID
		}
		return videos[i].CreateTime.UnixMilli() > videos[j].CreateTime.UnixMilli()
	})
}

func (f *FeedService) timelineResponse(ctx context.Context, videos []*video.Video, limit int, viewerAccountID uint) (ListLatestResponse, error) {
	hasMore := len(videos) > limit
	if hasMore {
		videos = videos[:limit]
	}
	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}
	nextCursor := ""
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextCursor = encodeTimelineCursor(TimelineCursor{
			CreateTime: last.CreateTime,
			VideoID:    last.ID,
		})
	}
	return ListLatestResponse{VideoList: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	feedVideos := make([]FeedVideoItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}

	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
}

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerID uint, cursor PopularCursor) (ListPopularityResponse, error) {
	if f.rediscache == nil {
		log.Printf("popularity feed Redis client unavailable, falling back to MySQL")
		return f.ListByPopularityFromDB(ctx, limit, cursor, viewerID)
	}

	// Redis 热榜（稳定分页：as_of + offset）
	asOf := time.Now().UTC().Truncate(time.Minute)
	if reqAsOf > 0 {
		asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
	}

	const win = 60
	keys := make([]string, win)
	for i := range keys {
		keys[i] = f.rediscache.HotVideoMinuteKey(asOf.Add(-time.Duration(i) * time.Minute))
	}

	key := f.rediscache.MergedHotVideoMinuteKey(asOf)
	opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()

	exists, _ := f.rediscache.Exists(opCtx, key)
	if !exists {
		_ = f.rediscache.ZUnionStore(opCtx, key, keys, "SUM")
		_ = f.rediscache.Expire(opCtx, key, 2*time.Minute) // 给翻页留时间
	}

	members, readErr := f.rediscache.ZRevRange(
		opCtx,
		key,
		int64(offset),
		int64(offset+limit-1),
	)
	if readErr == nil {
		if len(members) == 0 {
			return ListPopularityResponse{
				VideoList:  []FeedVideoItem{},
				AsOf:       asOf.Unix(),
				NextOffset: offset,
			}, nil
		}
		ids := make([]uint, 0, len(members))
		for _, member := range members {
			id, parseErr := strconv.ParseUint(member, 10, 64)
			if parseErr == nil && id > 0 {
				ids = append(ids, uint(id))
			}
		}
		videos, dbErr := f.GetVideoByIDs(ctx, ids)
		if dbErr != nil {
			return ListPopularityResponse{}, dbErr
		}
		return f.popularityResponse(ctx, videos, viewerID, asOf.Unix(), offset+len(members), len(members) == limit)
	}

	log.Printf("popularity feed Redis read failed, falling back to MySQL: %v", readErr)
	return f.ListByPopularityFromDB(ctx, limit, cursor, viewerID)
}

func (f *FeedService) ListByPopularityFromDB(ctx context.Context, limit int, cursor PopularCursor, viewerID uint) (ListPopularityResponse, error) {
	videos, err := f.repo.ListByPopularity(ctx, limit, cursor)
	if err != nil {
		return ListPopularityResponse{}, err
	}
	return f.popularityResponse(ctx, videos, viewerID, 0, 0, len(videos) == limit)
}

func (f *FeedService) popularityResponse(ctx context.Context, videos []*video.Video, viewerID uint, asOf int64, nextOffset int, hasMore bool) (ListPopularityResponse, error) {
	items, err := f.buildFeedVideos(ctx, videos, viewerID)
	if err != nil {
		return ListPopularityResponse{}, err
	}
	resp := ListPopularityResponse{
		VideoList:  items,
		AsOf:       asOf,
		NextOffset: nextOffset,
		HasMore:    hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		resp.NextCursor = encodePopularCursor(PopularCursor{
			Popularity: last.Popularity,
			CreateTime: last.CreateTime,
			VideoID:    last.ID,
		})
	}
	return resp, nil
}

func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exits := dataMap[id]; exits && v != nil {
			res = append(res, v)
		}
	}
	return res
}

const BigCreatorFollowerThreshold = 1000

type followingRelations struct {
	Authors     []uint `json:"authors"`
	BigCreators []uint `json:"big_creators"`
}

func (f *FeedService) getFollowingRelations(ctx context.Context, viewerID uint) (followingRelations, error) {
	key := f.rediscache.FollowingRelationsKey(viewerID)
	if cached, err := f.rediscache.GetBytes(ctx, key); err == nil {
		var relations followingRelations
		if json.Unmarshal(cached, &relations) == nil {
			return relations, nil
		}
	}
	authors, err := f.repo.ListFollowingAuthorIDs(ctx, viewerID)
	if err != nil {
		return followingRelations{}, err
	}
	relations := followingRelations{Authors: authors}
	if len(authors) > 0 {
		relations.BigCreators, err = f.repo.ListBigFollowingAuthorIDs(ctx, authors)
		if err != nil {
			return followingRelations{}, err
		}
	}
	if encoded, err := json.Marshal(relations); err == nil {
		_ = f.rediscache.SetBytes(ctx, key, encoded, 30*time.Second)
	}
	return relations, nil
}

// ListFollowing reads candidate IDs from the follower inbox and followed authors' outboxes.
func (f *FeedService) ListFollowing(ctx context.Context, limit int, cursor TimelineCursor, viewerID uint) (ListFollowingResponse, error) {
	if f.rediscache == nil {
		return ListFollowingResponse{}, errors.New("following feed Redis client unavailable")
	}
	if viewerID == 0 {
		return ListFollowingResponse{}, errors.New("login required")
	}
	relations, err := f.getFollowingRelations(ctx, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	authors := relations.Authors
	if len(authors) == 0 {
		return ListFollowingResponse{VideoList: []FeedVideoItem{}}, nil
	}
	bigCreators := relations.BigCreators
	followed := make(map[uint]struct{}, len(authors))
	for _, id := range authors {
		followed[id] = struct{}{}
	}
	ids, err := f.listFollowingRedis(ctx, viewerID, bigCreators, followed, cursor, limit)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	baseVideos, err := f.GetVideoByIDs(ctx, ids)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	sortFollowingVideos(baseVideos)
	return f.followingResponse(ctx, baseVideos, limit, viewerID)
}

func (f *FeedService) followingResponse(ctx context.Context, videos []*video.Video, limit int, viewerID uint) (ListFollowingResponse, error) {
	resp, err := f.timelineResponse(ctx, videos, limit, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	return ListFollowingResponse{
		VideoList:  resp.VideoList,
		NextCursor: resp.NextCursor,
		HasMore:    resp.HasMore,
	}, nil
}

// Read the follower inbox and all followed big-creator outboxes in one round trip.
func (f *FeedService) listFollowingRedis(ctx context.Context, viewerID uint, bigCreators []uint, followed map[uint]struct{}, cursor TimelineCursor, limit int) ([]uint, error) {
	pipe := f.rediscache.Pipeline()
	if pipe == nil {
		return nil, errors.New("following feed Redis pipeline unavailable")
	}
	commands := make([]*redis.ZSliceCmd, 0, len(bigCreators)+1)
	commands = append(commands, pipe.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     f.rediscache.FollowerInboxKey(viewerID),
		Start:   followingMaxScore(cursor),
		Stop:    "-inf",
		ByScore: true,
		Rev:     true,
		Count:   int64(limit + 1),
	}))
	for _, authorID := range bigCreators {
		commands = append(commands, pipe.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
			Key:     f.rediscache.AuthorOutboxKey(authorID),
			Start:   followingMaxScore(cursor),
			Stop:    "-inf",
			ByScore: true,
			Rev:     true,
			Count:   int64(limit + 1),
		}))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("read following Redis indexes: %w", err)
	}
	type candidate struct {
		id    uint
		score float64
	}
	candidates := make([]candidate, 0, (limit+1)*(len(bigCreators)+1))
	seen := make(map[uint]struct{}, cap(candidates))
	for _, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil, fmt.Errorf("get following Redis index result: %w", err)
		}
		for _, value := range values {
			member, ok := value.Member.(string)
			if !ok {
				continue
			}
			authorID, videoID, ok := parseFeedVideoMember(member)
			if !ok {
				continue
			}
			if _, ok := followed[authorID]; !ok {
				continue
			}
			if _, duplicate := seen[videoID]; duplicate {
				continue
			}
			seen[videoID] = struct{}{}
			candidates = append(candidates, candidate{id: videoID, score: value.Score})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].id > candidates[j].id
		}
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) > limit+1 {
		candidates = candidates[:limit+1]
	}
	ids := make([]uint, 0, len(candidates))
	for _, item := range candidates {
		ids = append(ids, item.id)
	}
	return ids, nil
}

func followingMaxScore(cursor TimelineCursor) string {
	if cursor.CreateTime.IsZero() {
		return "+inf"
	}
	return strconv.FormatInt(cursor.CreateTime.UnixMilli(), 10)
}

func beforeFollowingCursor(created time.Time, videoID uint, cursor TimelineCursor) bool {
	return cursor.CreateTime.IsZero() || created.Before(cursor.CreateTime) ||
		(created.Equal(cursor.CreateTime) && videoID < cursor.VideoID)
}

func sortFollowingVideos(videos []*video.Video) {
	sort.Slice(videos, func(i, j int) bool {
		if videos[i].CreateTime.Equal(videos[j].CreateTime) {
			return videos[i].ID > videos[j].ID
		}
		return videos[i].CreateTime.After(videos[j].CreateTime)
	})
}

func parseFeedVideoMember(member string) (uint, uint, bool) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	authorID, err1 := strconv.ParseUint(parts[0], 10, 64)
	videoID, err2 := strconv.ParseUint(parts[1], 10, 64)
	return uint(authorID), uint(videoID), err1 == nil && err2 == nil && authorID > 0 && videoID > 0
}

func parseTimelineCursor(raw string) (TimelineCursor, error) {
	if raw == "" {
		return TimelineCursor{}, nil
	}
	content, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return TimelineCursor{}, err
	}
	var cursor struct {
		CreateTime time.Time `json:"create_time"`
		VideoID    uint      `json:"video_id"`
	}
	if err := json.Unmarshal(content, &cursor); err != nil || cursor.CreateTime.IsZero() || cursor.VideoID == 0 {
		return TimelineCursor{}, err
	}
	return TimelineCursor{CreateTime: cursor.CreateTime, VideoID: cursor.VideoID}, nil
}

func encodeTimelineCursor(cursor TimelineCursor) string {
	content, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(content)
}

func parsePopularCursor(raw string) (PopularCursor, error) {
	if raw == "" {
		return PopularCursor{}, nil
	}
	content, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return PopularCursor{}, err
	}
	var cursor PopularCursor
	if err := json.Unmarshal(content, &cursor); err != nil || cursor.Popularity < 0 || cursor.CreateTime.IsZero() || cursor.VideoID == 0 {
		return PopularCursor{}, err
	}
	return cursor, nil
}

func encodePopularCursor(cursor PopularCursor) string {
	content, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(content)
}
