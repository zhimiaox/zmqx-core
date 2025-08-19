package redis

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Session = (*session)(nil)

func (p *persistenceImpl) newSession() *session {
	s := &session{rdb: p.rdb}
	s.tw = common.NewRedisClock(sessionClockKey, p.rdb, func(clientID string) {
		if s.handler != nil {
			s.handler(clientID)
		}
	}, p.logger)
	return s
}

type session struct {
	rdb       redis.UniversalClient
	handler   func(clientID string)
	tw        common.RedisClock
	onceStart sync.Once
}

func (s *session) Event(handler func(clientID string)) {
	s.handler = handler
	s.onceStart.Do(func() {
		go s.tw.Start()
	})
}

func (s *session) Set(ses *models.Session) error {
	b := &bytes.Buffer{}
	models.EncodeSession(ses, b)
	err := s.rdb.HSet(context.Background(), sessionKey, ses.ClientID, b.String()).Err()
	if err != nil {
		return err
	}
	if !ses.ExpiryAt.IsZero() {
		if err = s.tw.Set(ses.ClientID, ses.ExpiryAt.Add(time.Second)); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) Remove(clientID string) error {
	_ = s.tw.Del(clientID)
	return s.rdb.HDel(context.Background(), sessionKey, clientID).Err()
}

func (s *session) Get(clientID string) (*models.Session, error) {
	result, err := s.rdb.HGet(context.Background(), sessionKey, clientID).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if result == "" {
		return nil, nil
	}
	return models.DecodeSession(bytes.NewBufferString(result))
}
