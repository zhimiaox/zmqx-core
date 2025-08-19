package redis

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Subscription = (*subscription)(nil)

// subscription implement the subscription.Interface, it use trie tree to store topics.
type subscription struct {
	rdb    redis.UniversalClient
	logger *slog.Logger
}

// Subscribe
// topic/ss/sss -> [sub binary]
// topic/ss -> sss
func (s *subscription) Subscribe(clientID string, subs ...*models.Subscription) ([]models.SubscribeResult, error) {
	ctx := context.Background()
	rs := make([]models.SubscribeResult, 0)
	for _, sub := range subs {
		result, err := s.rdb.SAdd(ctx, subscriptionClientIdxKey+clientID, sub.GetFullTopicName()).Result()
		if err != nil {
			return nil, err
		}
		rs = append(rs, models.SubscribeResult{
			Subscription:   sub,
			AlreadyExisted: result == 0,
		})
		topicSlice := strings.Split(sub.TopicFilter, "/")
		tireKey := &strings.Builder{}
		tireKey.WriteString(subscriptionTrieKey)
		for i := range topicSlice {
			if i == 0 {
				if err = s.rdb.SAdd(ctx, subscriptionTrieRootKey, topicSlice[0]).Err(); err != nil {
					s.logger.Error("subscribe tire root store err", "topic_slice", topicSlice[0], "err", err)
					return nil, err
				}
			}
			if i+1 == len(topicSlice) {
				break
			}
			if i > 0 {
				tireKey.WriteByte('/')
			}
			tireKey.WriteString(topicSlice[i])
			if err = s.rdb.SAdd(ctx, tireKey.String(), topicSlice[i+1]).Err(); err != nil {
				s.logger.Error("subscribe tire store err",
					"tire_key", tireKey.String(), "topic_slice", topicSlice[i+1], "err", err)
				return nil, err
			}
		}
		subRaw := &bytes.Buffer{}
		models.EncodeSubscription(sub, subRaw)
		if sub.ShareName != "" {
			if err = s.rdb.HSet(ctx, subscriptionDataShareKey+sub.TopicFilter, fmt.Sprintf("%s#%s", sub.ShareName, clientID), subRaw.String()).Err(); err != nil {
				s.logger.Error("subscribe data store err", "topic_filter", sub.TopicFilter, "err", err)
				return nil, err
			}
		} else if common.IsSystemTopic(sub.TopicFilter) {
			if err = s.rdb.HSet(ctx, subscriptionDataSysKey+sub.TopicFilter, clientID, subRaw.String()).Err(); err != nil {
				s.logger.Error("subscribe data store err", "topic_filter", sub.TopicFilter, "err", err)
				return nil, err
			}
		} else {
			if err = s.rdb.HSet(ctx, subscriptionDataUserKey+sub.TopicFilter, clientID, subRaw.String()).Err(); err != nil {
				s.logger.Error("subscribe data store err", "topic_filter", sub.TopicFilter, "err", err)
				return nil, err
			}
		}
	}
	return rs, nil
}

func (s *subscription) Unsubscribe(clientID string, topics ...string) error {
	ctx := context.Background()
	var lastErr error
	for i := range topics {
		shareName, topicFilter := common.SplitTopic(topics[i])
		if shareName != "" {
			if err := s.rdb.HDel(ctx, subscriptionDataShareKey+topicFilter, fmt.Sprintf("%s#%s", shareName, clientID)).Err(); err != nil {
				s.logger.Error("Unsubscribe data share topic err",
					"client_id", clientID, "share_name", shareName, "topic_filter", topicFilter, "err", err)
				lastErr = err
			}
		} else if common.IsSystemTopic(topicFilter) {
			if err := s.rdb.HDel(ctx, subscriptionDataSysKey+topicFilter, clientID).Err(); err != nil {
				s.logger.Error("Unsubscribe data sys topic err",
					"client_id", clientID, "topic_filter", topicFilter, "err", err)
				lastErr = err
			}
		} else {
			if err := s.rdb.HDel(ctx, subscriptionDataUserKey+topicFilter, clientID).Err(); err != nil {
				s.logger.Error("Unsubscribe data user topic err",
					"client_id", clientID, "topic_filter", topicFilter, "err", err)
				lastErr = err
			}
		}
		if err := s.rdb.SRem(ctx, subscriptionClientIdxKey+clientID, topics[i]).Err(); err != nil {
			s.logger.Error("Unsubscribe data client idx err",
				"client_id", clientID, "topic_filter", topics[i], "err", err)
			lastErr = err
		}
	}
	return lastErr
}

func (s *subscription) UnsubscribeAll(clientID string) error {
	topics := s.rdb.SMembers(context.Background(), subscriptionClientIdxKey+clientID).Val()
	if len(topics) > 0 {
		return s.Unsubscribe(clientID, topics...)
	}
	return nil
}

func (s *subscription) Iterate(fn models.SubscriptionIterateFn, options models.IterationOptions) {
	ctx := context.Background()
	topicFilterSlice := strings.Split(options.TopicName, "/")
	endMathFilter := make([]string, 0)
	matchFilter := make([]string, 0)
	for i, filterNode := range topicFilterSlice {
		if i == 0 {
			if s.rdb.SIsMember(ctx, subscriptionTrieRootKey, filterNode).Val() {
				matchFilter = append(matchFilter, filterNode)
			}
			if s.rdb.SIsMember(ctx, subscriptionTrieRootKey, "+").Val() {
				matchFilter = append(matchFilter, "+")
			}
			if s.rdb.SIsMember(ctx, subscriptionTrieRootKey, "#").Val() {
				endMathFilter = append(endMathFilter, "#")
			}
		} else {
			tempMath := make([]string, 0)
			for i2 := range matchFilter {
				if s.rdb.SIsMember(ctx, subscriptionTrieKey+matchFilter[i2], filterNode).Val() {
					tempMath = append(tempMath, matchFilter[i2]+"/"+filterNode)
				}
				if s.rdb.SIsMember(ctx, subscriptionTrieKey+matchFilter[i2], "+").Val() {
					tempMath = append(tempMath, matchFilter[i2]+"/+")
				}
				if s.rdb.SIsMember(ctx, subscriptionTrieKey+matchFilter[i2], "#").Val() {
					endMathFilter = append(endMathFilter, matchFilter[i2]+"/#")
				}
			}
			matchFilter = tempMath
		}
	}
	// 贪婪#检测, 以使得 t1->t1/#, t1/a->t1/a/#
	for i := range matchFilter {
		if s.rdb.SIsMember(ctx, subscriptionTrieKey+matchFilter[i], "#").Val() {
			endMathFilter = append(endMathFilter, matchFilter[i]+"/#")
		}
	}
	matchFilter = append(matchFilter, endMathFilter...)
	for _, filterTopic := range matchFilter {
		if options.Type&consts.TypeShared > 0 {
			for key, subRaw := range s.rdb.HGetAll(ctx, subscriptionDataShareKey+filterTopic).Val() {
				sub, err := models.DecodeSubscription(bytes.NewBufferString(subRaw))
				if err != nil {
					s.logger.Error("decode subscription", "err", err)
					continue
				}
				fn(strings.SplitN(key, "#", 2)[1], sub)
			}
		}
		if options.Type&consts.TypeNonShared > 0 {
			for key, subRaw := range s.rdb.HGetAll(ctx, subscriptionDataUserKey+filterTopic).Val() {
				sub, err := models.DecodeSubscription(bytes.NewBufferString(subRaw))
				if err != nil {
					s.logger.Error("decode subscription", "err", err)
					continue
				}
				fn(key, sub)
			}
		}
		if options.Type&consts.TypeSYS > 0 {
			for key, subRaw := range s.rdb.HGetAll(ctx, subscriptionDataSysKey+filterTopic).Val() {
				sub, err := models.DecodeSubscription(bytes.NewBufferString(subRaw))
				if err != nil {
					s.logger.Error("decode subscription", "err", err)
					continue
				}
				fn(key, sub)
			}
		}
	}
}

// newSubscription create a new subscription instance
func (p *persistenceImpl) newSubscription() *subscription {
	return &subscription{
		rdb:    p.rdb,
		logger: p.logger.With("persistence", "subscription"),
	}
}
