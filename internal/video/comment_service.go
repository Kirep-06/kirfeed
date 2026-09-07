package video

import (
	"context"
	"errors"
	"kirfeed/internal/apierror"
	rediscache "kirfeed/internal/infra/cache"
	"kirfeed/internal/infra/mq"
	"strings"

	"gorm.io/gorm"
)

type CommentService struct {
	repo            *CommentRepository
	VideoRepository *VideoRepository
	cache           *rediscache.Client
	commentMQ       *mq.CommentMQ
	popularityMQ    *mq.PopularityMQ
}

func NewCommentService(repo *CommentRepository, videoRepo *VideoRepository, cache *rediscache.Client, commentMQ *mq.CommentMQ, popularityMQ *mq.PopularityMQ) *CommentService {
	return &CommentService{
		repo:            repo,
		VideoRepository: videoRepo,
		cache:           cache,
		commentMQ:       commentMQ,
		popularityMQ:    popularityMQ,
	}
}

func (s *CommentService) Publish(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return apierror.New(apierror.ErrValidation, "comment is nil")
	}
	comment.Username = strings.TrimSpace(comment.Username)
	comment.Content = strings.TrimSpace(comment.Content)
	
	if comment.VideoID == 0 || comment.AuthorID == 0 {
		return apierror.New(apierror.ErrValidation, "video_id and author_id are required")
	}
	if comment.Content == "" {
		return apierror.New(apierror.ErrValidation, "content is required")
	}

	exists, err := s.VideoRepository.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return apierror.New(apierror.ErrNotFound, "video not found")
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.commentMQ != nil {
		if err := s.commentMQ.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// Fallback: direct MySQL write when comment MQ publish fails.
	if !mysqlEnqueued {
		if err := s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Select("id").First(&Video{}, comment.VideoID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return apierror.New(apierror.ErrNotFound, "video not found")
				}
				return err
			}
			if err := tx.Create(comment).Error; err != nil {
				return err
			}
			return tx.Model(&Video{}).Where("id = ?", comment.VideoID).
				UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error
		}); err != nil {
			return err
		}
	}

	// Fallback: direct Redis update when popularity MQ publish fails.
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, comment.VideoID, 1)
	}
	return nil
}

func (s *CommentService) Delete(ctx context.Context, commentID uint, accountID uint) error {
	comment, err := s.repo.GetByID(ctx, commentID)
	if err != nil {
		return err
	}
	if comment == nil {
		return apierror.New(apierror.ErrNotFound, "comment not found")
	}
	if comment.AuthorID != accountID {
		return apierror.ErrUnauthorized
	}
	if s.commentMQ != nil {
		if err := s.commentMQ.Delete(ctx, commentID); err == nil {
			return nil
		}
	}
	return s.repo.DeleteComment(ctx, comment)
}

func (s *CommentService) GetAll(ctx context.Context, videoID uint) ([]Comment, error) {
	exists, err := s.VideoRepository.IsExist(ctx, videoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierror.New(apierror.ErrNotFound, "video not found")
	}
	return s.repo.GetAllComments(ctx, videoID)
}
