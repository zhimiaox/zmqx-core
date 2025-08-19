package common

import (
	"context"
	"time"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

type RedisClock interface {
	Del(id string) error
	Set(id string, clock time.Time) error
	Start()
	Stop()
}

type redisClockImpl struct {
	redis    redis.UniversalClient
	callback func(string)
	key      string
	done     chan struct{}
	timer    *time.Timer
	id       string
	logger   *slog.Logger
}

func (s *redisClockImpl) Start() {
	for {
		select {
		case <-s.timer.C:
			id := s.id
			if s.redis.ZRem(context.Background(), s.key, id).Val() > 0 {
				go s.callback(id)
			}
			s.reloadFirst()
		case <-s.done:
			s.timer.Stop()
			return
		}
	}
}

func (s *redisClockImpl) Stop() {
	close(s.done)
}

// Del  闹钟删除
func (s *redisClockImpl) Del(id string) error {
	res := s.redis.ZRem(context.Background(), s.key, id)
	if res.Err() != nil {
		return res.Err()
	}
	// 有效删除重新加载数据
	if res.Val() > 0 {
		s.reloadFirst()
	}
	return nil
}

// Set  闹钟设置
func (s *redisClockImpl) Set(id string, clock time.Time) error {
	// 间隔过短直接提醒，不再入库
	if time.Until(clock) < time.Second {
		go s.callback(id)
		return nil
	}
	if err := s.redis.ZAdd(context.Background(), s.key, redis.Z{
		Score:  float64(clock.Unix()),
		Member: id,
	}).Err(); err != nil {
		return err
	}
	s.reloadFirst()
	return nil
}

// 加载最近一条提醒
func (s *redisClockImpl) reloadFirst() {
	result, err := s.redis.ZRangeWithScores(context.Background(), s.key, 0, 1).Result()
	if err != nil {
		s.logger.Error("sticky notes alarm clock z range with scores", err)
	}
	// 无待提醒便签则挂起
	if len(result) == 0 {
		return
	}
	if id, ok := result[0].Member.(string); ok {
		s.id = id
	}
	s.timer.Reset(time.Duration(max(int64(result[0].Score)-time.Now().Unix(), 0)) * time.Second)
}

func NewRedisClock(key string, rdb redis.UniversalClient, callback func(id string), logger *slog.Logger) RedisClock {
	return &redisClockImpl{
		done:     make(chan struct{}),
		key:      key,
		callback: callback,
		redis:    rdb,
		timer:    time.NewTimer(0),
		logger:   logger,
	}
}
