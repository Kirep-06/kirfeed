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
	"sync"
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

	//L3:MySQL
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()
			sfKey := f.rediscache.VideoEntitySingleflightKey(videoID)

			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})

				if err != nil || len(videoList) == 0 {
					return nil, err
				}

				safeCopy := *videoList[0]
				cachekey := f.rediscache.VideoEntityKey(safeCopy.ID)
				if b, err := json.Marshal(safeCopy); err == nil {
					//异步回写redis
					go func(k string, b []byte) {
						setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
						defer setCancel()

						f.rediscache.SetBytes(setCtx, k, b, time.Hour)
					}(cachekey, b)
				}
				return videoList[0], err
			})

			if err == nil && v != nil {
				safeCopy := *(v.(*video.Video))
				mu.Lock()
				videoMap[id] = &safeCopy
				mu.Unlock()
				f.localcache.Set(f.rediscache.VideoEntityKey(safeCopy.ID), safeCopy, 5*time.Second)
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

// 查询最新视频 (冷热分离 + 游标分页)
func (f *FeedService) ListLatest(ctx context.Context, limit int, cursor TimelineCursor, viewerAccountID uint) (ListLatestResponse, error) {

	if f.rediscache == nil {
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	// 获取 ZSET 中最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, f.rediscache.GlobalTimelineKey(), 0, 0)
	if err != nil {
		return f.listLatestFromDB(ctx, limit, cursor, viewerAccountID)
	}

	isZsetEmpty := len(zsetTail) == 0

	if isZsetEmpty {
		sfKey := f.rediscache.GlobalTimelineKey()

		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			// 无视游标，直接去 MySQL 捞最新的 1000 条
			dbVideos, err := f.repo.ListLatest(ctx, 1000, cursor)
			if err != nil {
				return nil, err
			}
			if len(dbVideos) == 0 {
				return "EMPTY_DB", nil // 防无限递归
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
			f.rediscache.ZAdd(bgCtx, f.rediscache.Key("feed:global_timeline"), zElements...)
			return "SUCCESS", nil
		})

		if err != nil {
			return ListLatestResponse{}, err
		}
		if v == "EMPTY_DB" {
			return ListLatestResponse{HasMore: false}, nil
		}

		// 让所有被阻塞的请求重新查一遍
		return f.ListLatest(ctx, limit, cursor, viewerAccountID)
	}

	watermark := int64(zsetTail[0].Score)
	reqTime := time.Now().UnixMilli()
	if !cursor.CreateTime.IsZero() {
		reqTime = cursor.CreateTime.UnixMilli()
	}

	var baseVideos []*video.Video

	if reqTime <= watermark {
		//冷数据降级查库

		// 针对个别用户的防并发（此时可以用时间戳做锁，因为冷尾流量极小）
		sfKey := f.rediscache.GlobalTimelineKey()
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			return f.repo.ListLatest(ctx, limit, cursor)
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 不回写 ZSET，防止冷数据污染热点时间线
	} else {
		// 热数据直接查redis
		videoIDs, err := f.latestRedisIDs(ctx, limit, cursor)
		if err != nil {
			return ListLatestResponse{}, err
		}

		if len(videoIDs) > 0 {
			baseVideos, err = f.GetVideoByIDs(ctx, videoIDs)
			if err != nil {
				return ListLatestResponse{}, err
			}
		}

		if len(baseVideos) < limit {
			remainLimit := limit - len(baseVideos) // 计算还差几个

			var coldCursor TimelineCursor
			if len(baseVideos) > 0 {
				coldCursor = TimelineCursor{
					CreateTime: baseVideos[len(baseVideos)-1].CreateTime,
					VideoID:    baseVideos[len(baseVideos)-1].ID,
				}
			} else {
				coldCursor = TimelineCursor{
					CreateTime: cursor.CreateTime,
					VideoID:    cursor.VideoID,
				}
			}

			sfKey := f.rediscache.GlobalTimelineKey()
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				return f.repo.ListLatest(ctx, remainLimit, coldCursor)
			})

			if err == nil {
				coldVideos := v.([]*video.Video)
				baseVideos = append(baseVideos, coldVideos...)
			}
		}
	}

	var nextCursor TimelineCursor
	if len(baseVideos) > 0 {
		// 将本页最后一条视频的时间作为下一次请求的游标
		nextCursor = TimelineCursor{
			CreateTime: baseVideos[len(baseVideos)-1].CreateTime,
			VideoID:    baseVideos[len(baseVideos)-1].ID,
		}
	}
	hasMore := len(baseVideos) == limit

	feedVideos, err := f.buildFeedVideos(ctx, baseVideos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}

	return ListLatestResponse{
		VideoList:  feedVideos,
		NextCursor: encodeTimelineCursor(nextCursor),
		HasMore:    hasMore,
	}, nil
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
		int64(limit+1),
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
	return f.latestResponse(ctx, videos, limit, viewerAccountID)
}

func sortLatestVideos(videos []*video.Video) {
	sort.Slice(videos, func(i, j int) bool {
		if videos[i].CreateTime.UnixMilli() == videos[j].CreateTime.UnixMilli() {
			return videos[i].ID > videos[j].ID
		}
		return videos[i].CreateTime.UnixMilli() > videos[j].CreateTime.UnixMilli()
	})
}

func (f *FeedService) latestResponse(ctx context.Context, videos []*video.Video, limit int, viewerAccountID uint) (ListLatestResponse, error) {
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

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerID uint, cursor PopularCursor) (ListByPopularityResponse, error) {
	if limit <= 0 || limit > 50 {
		limit = DefaultLimit
	}

	// Redis 热榜（稳定分页：as_of + offset）
	if f.rediscache != nil {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
		}
		keys := make([]string, 60)
		for i := range keys {
			keys[i] = f.rediscache.HotVideoMinuteKey(asOf.Add(-time.Duration(i) * time.Minute))
		}
		dest := f.rediscache.MergedHotVideoMinuteKey(asOf)
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, err := f.rediscache.Exists(opCtx, dest)
		if err == nil && !exists {
			err = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			if err == nil {
				err = f.rediscache.Expire(opCtx, dest, 2*time.Minute)
			}
		}
		if err == nil {
			members, readErr := f.rediscache.ZRevRange(opCtx, dest, int64(offset), int64(offset+limit-1))
			if readErr == nil {
				if len(members) == 0 && offset > 0 {
					return ListByPopularityResponse{VideoList: []FeedVideoItem{}, AsOf: asOf.Unix(), NextOffset: offset}, nil
				}
				if len(members) > 0 {
					ids := make([]uint, 0, len(members))
					for _, member := range members {
						id, parseErr := strconv.ParseUint(member, 10, 64)
						if parseErr == nil && id > 0 {
							ids = append(ids, uint(id))
						}
					}
					videos, dbErr := f.repo.GetByIDs(ctx, ids)
					if dbErr == nil {
						byID := make(map[uint]*video.Video, len(videos))
						for _, v := range videos {
							byID[v.ID] = v
						}
						ordered := make([]*video.Video, 0, len(ids))
						for _, id := range ids {
							if v := byID[id]; v != nil {
								ordered = append(ordered, v)
							}
						}
						items, buildErr := f.buildFeedVideos(ctx, ordered, viewerID)
						if buildErr != nil {
							return ListByPopularityResponse{}, buildErr
						}
						resp := ListByPopularityResponse{VideoList: items, AsOf: asOf.Unix(), NextOffset: offset + len(members), HasMore: len(members) == limit}
						setPopularityCursor(&resp, ordered)
						return resp, nil
					}
				}
			}
		}
	}
	videos, err := f.repo.ListByPopularity(ctx, limit, cursor)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	items, err := f.buildFeedVideos(ctx, videos, viewerID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	resp := ListByPopularityResponse{VideoList: items, HasMore: len(videos) == limit}
	setPopularityCursor(&resp, videos)
	return resp, nil
}

func setPopularityCursor(resp *ListByPopularityResponse, videos []*video.Video) {
	if len(videos) == 0 {
		return
	}
	last := videos[len(videos)-1]
	resp.NextCursor = encodePopularCursor(PopularCursor{Popularity: last.Popularity, CreateTime: last.CreateTime, VideoID: last.ID})
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

var ErrInvalidFollowingCursor = errors.New("invalid following cursor")
var ErrInvalidPopularCursor = errors.New("invalid popular cursor")

func (f *FeedService) listFollowingFromDB(ctx context.Context, limit int, cursor TimelineCursor, viewerID uint) (ListFollowingResponse, error) {
	if viewerID == 0 {
		return ListFollowingResponse{}, errors.New("login required")
	}
	videos, err := f.repo.ListFollowingVideos(ctx, viewerID, &cursor, limit+1)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	hasMore := len(videos) > limit
	if hasMore {
		videos = videos[:limit]
	}
	items, err := f.buildFeedVideos(ctx, videos, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	nextCursor := ""
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextCursor = encodeTimelineCursor(TimelineCursor{
			CreateTime: last.CreateTime,
			VideoID:    last.ID,
		})
	}
	return ListFollowingResponse{VideoList: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

// ListFollowing merges Redis indexes with MySQL for cold or incomplete pages.
func (f *FeedService) ListFollowing(ctx context.Context, limit int, cursor TimelineCursor, viewerID uint) (ListFollowingResponse, error) {
	if limit <= 0 || limit > 50 {
		limit = DefaultLimit
	}

	if f.rediscache == nil {
		return f.listFollowingFromDB(ctx, limit, cursor, viewerID)
	}
	if viewerID == 0 {
		return ListFollowingResponse{}, errors.New("login required")
	}
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	authors, err := f.repo.ListFollowingAuthorIDs(ctx, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	if len(authors) == 0 {
		return ListFollowingResponse{VideoList: []FeedVideoItem{}}, nil
	}
	bigCreators, err := f.repo.ListBigFollowingAuthorIDs(ctx, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	followed := make(map[uint]struct{}, len(authors))
	for _, id := range authors {
		followed[id] = struct{}{}
	}
	ids := f.listFollowingRedis(ctx, viewerID, bigCreators, followed)
	videos, err := f.GetVideoByIDs(ctx, ids)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	valid := make([]*video.Video, 0, len(videos))
	seen := make(map[uint]struct{}, len(videos))
	for _, item := range videos {
		if item == nil || !beforeFollowingCursor(item.CreateTime, item.ID, cursor.CreateTime, cursor.VideoID) {
			continue
		}
		if _, ok := followed[item.AuthorID]; ok {
			seen[item.ID] = struct{}{}
			valid = append(valid, item)
		}
	}
	dbCursor := cursor
	// Always read the first DB page so a delayed Redis fanout cannot hide newer videos.
	for firstPage := true; firstPage || len(valid) < limit+1; firstPage = false {
		page, err := f.repo.ListFollowingVideos(ctx, viewerID, &dbCursor, limit+1)
		if err != nil {
			return ListFollowingResponse{}, err
		}
		for _, item := range page {
			if _, exists := seen[item.ID]; !exists {
				seen[item.ID] = struct{}{}
				valid = append(valid, item)
			}
		}
		if len(page) < limit+1 {
			break
		}
		last := page[len(page)-1]
		dbCursor = TimelineCursor{CreateTime: last.CreateTime, VideoID: last.ID}
	}
	sort.Slice(valid, func(i, j int) bool {
		if valid[i].CreateTime.UnixMilli() == valid[j].CreateTime.UnixMilli() {
			return valid[i].ID > valid[j].ID
		}
		return valid[i].CreateTime.UnixMilli() > valid[j].CreateTime.UnixMilli()
	})
	if len(valid) > limit+1 {
		valid = valid[:limit+1]
	}
	hasMore := len(valid) > limit
	if hasMore {
		valid = valid[:limit]
	}
	items, err := f.buildFeedVideos(ctx, valid, viewerID)
	if err != nil {
		return ListFollowingResponse{}, err
	}
	nextCursor := ""
	if len(valid) > 0 {
		last := valid[len(valid)-1]
		nextCursor = encodeTimelineCursor(TimelineCursor{
			CreateTime: last.CreateTime,
			VideoID:    last.ID,
		})
	}
	return ListFollowingResponse{VideoList: items, NextCursor: nextCursor, HasMore: hasMore}, nil
}

func (f *FeedService) listFollowingRedis(ctx context.Context, viewerID uint, bigCreators []uint, followed map[uint]struct{}) []uint {
	keys := []string{f.rediscache.FollowerInboxKey(viewerID)}
	for _, authorID := range bigCreators {
		keys = append(keys, f.rediscache.AuthorOutboxKey(authorID))
	}
	pipe := f.rediscache.Pipeline()
	if pipe == nil {
		return nil
	}
	commands := make([]*redis.ZSliceCmd, 0, len(keys))
	for _, key := range keys {
		commands = append(commands, pipe.ZRevRangeWithScores(ctx, key, 0, 499))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil
	}
	seen := make(map[uint]struct{})
	ids := make([]uint, 0)
	for source, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil
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
			if _, ok := followed[authorID]; !ok || (source > 0 && authorID != bigCreators[source-1]) {
				continue
			}
			if _, duplicate := seen[videoID]; duplicate {
				continue
			}
			seen[videoID] = struct{}{}
			ids = append(ids, videoID)
		}
	}
	return ids
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

func beforeFollowingCursor(created time.Time, videoID uint, before time.Time, beforeID uint) bool {
	return before.IsZero() || created.UnixMilli() < before.UnixMilli() ||
		(created.UnixMilli() == before.UnixMilli() && videoID < beforeID)
}

func parseTimelineCursor(raw string) (TimelineCursor, error) {
	if raw == "" {
		return TimelineCursor{}, nil
	}
	content, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return TimelineCursor{}, ErrInvalidFollowingCursor
	}
	var cursor struct {
		CreateTime time.Time `json:"create_time"`
		VideoID    uint      `json:"video_id"`
	}
	if err := json.Unmarshal(content, &cursor); err != nil || cursor.CreateTime.IsZero() || cursor.VideoID == 0 {
		return TimelineCursor{}, ErrInvalidFollowingCursor
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
		return PopularCursor{}, ErrInvalidPopularCursor
	}
	var cursor PopularCursor
	if err := json.Unmarshal(content, &cursor); err != nil || cursor.Popularity < 0 || cursor.CreateTime.IsZero() || cursor.VideoID == 0 {
		return PopularCursor{}, ErrInvalidPopularCursor
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
