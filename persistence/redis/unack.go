package redis

import (
	"context"
	"fmt"

	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/persistence"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Unack = (*unack)(nil)

type unack struct {
	rdb      redis.UniversalClient
	clientID string
	key      string
}

func (s *unack) Clean() error {
	return s.rdb.Del(context.Background(), s.key).Err()
}

func (p *persistenceImpl) newUnack(clientID string) *unack {
	return &unack{
		rdb:      p.rdb,
		clientID: clientID,
		key:      fmt.Sprintf(unackKey, clientID),
	}
}

func (s *unack) Init(cleanStart bool) error {
	if cleanStart {
		if err := s.Clean(); err != nil {
			return err
		}
	}
	return nil
}

func (s *unack) Set(id packets.PacketID) (bool, error) {
	result, err := s.rdb.SAdd(context.Background(), s.key, id).Result()
	return result == 0, err
}

func (s *unack) Remove(id packets.PacketID) error {
	return s.rdb.SRem(context.Background(), s.key, id).Err()
}
