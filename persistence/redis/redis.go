package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/config"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

const (
	// LockKey client_id lock
	LockKey = consts.GlobalPrefix + ":lock:%s"
	// EventQueueChannel 事件-队列消息
	EventQueueChannel = consts.GlobalPrefix + ":event:queue"
	// client_id => [queue elem] 当前队列消息
	queueCurrentKey = consts.GlobalPrefix + ":queue:current:%s"
	// package id => queue elem 已发送的有id的飞行消息
	queueInflightKey = consts.GlobalPrefix + ":queue:inflight:%s"
	// topic => msg 保留消息
	retainedDataKey = consts.GlobalPrefix + ":retained:data:"
	// topic => msg 保留消息
	retainedTrieKey = consts.GlobalPrefix + ":retained:trie:"
	// topic => msg 保留消息
	retainedTrieRootKey = consts.GlobalPrefix + ":retained:trie-root"
	// client_id => session
	sessionKey = consts.GlobalPrefix + ":session"
	// client_id => session
	sessionClockKey = consts.GlobalPrefix + ":session-clock"
	// key:/topics/# client_id => [sub]
	subscriptionDataUserKey = consts.GlobalPrefix + ":subscription:data-user:"
	// key:/topics/# client_id => [sub]
	subscriptionDataSysKey = consts.GlobalPrefix + ":subscription:data-sys:"
	// key:/topics/# client_id => [sub]
	subscriptionDataShareKey = consts.GlobalPrefix + ":subscription:data-share:"
	// client_id [full topic] 客户端所有订阅
	subscriptionClientIdxKey = consts.GlobalPrefix + ":subscription:client:"
	// key:/topic => #
	subscriptionTrieKey = consts.GlobalPrefix + ":subscription:trie:"
	// key => [ ]
	subscriptionTrieRootKey = consts.GlobalPrefix + ":subscription:trie-root"
	// client_id => [package_id]
	unackKey = consts.GlobalPrefix + ":unack:%s"
	// client_id => will data
	willDataKey = consts.GlobalPrefix + ":will"
	// client_id => time source
	willClockKey = consts.GlobalPrefix + ":will-clock"
)

type persistenceImpl struct {
	rdb redis.UniversalClient

	sessionStore       persistence.Session
	subscriptionsStore persistence.Subscription
	retainedStore      persistence.Retained
	willStore          persistence.Will

	queueStore sync.Map
	unackStore sync.Map
	cfg        *config.Config
	logger     *slog.Logger
}

func (p *persistenceImpl) DestroyClient(clientID string) {
	logger := p.logger.With("client_id", clientID)
	// clean session
	if err := p.sessionStore.Remove(clientID); err != nil {
		logger.Error("remove session", "err", err)
	}
	// clean up queue
	if value, ok := p.queueStore.LoadAndDelete(clientID); ok {
		if val, ok := value.(*queue); ok {
			if err := val.Clean(); err != nil {
				logger.Error("remove session queue", "err", err)
			}
		}
	}
	// clean up unack id
	if value, ok := p.unackStore.LoadAndDelete(clientID); ok {
		if val, ok := value.(*unack); ok {
			if err := val.Clean(); err != nil {
				logger.Error("remove unack store", "err", err)
			}
		}
	}
	// clean up the subscription
	if err := p.subscriptionsStore.UnsubscribeAll(clientID); err != nil {
		logger.Error("remove session unsubscribe all", "err", err)
	}
}

func (p *persistenceImpl) Unlock(clientID string) {
	p.rdb.Del(context.Background(), fmt.Sprintf(LockKey, clientID))
}

func (p *persistenceImpl) TryLock(clientID string) bool {
	return p.rdb.SetNX(context.Background(), fmt.Sprintf(LockKey, clientID), "", time.Minute).Val()
}

func (p *persistenceImpl) Will() persistence.Will {
	return p.willStore
}

func (p *persistenceImpl) Subscription() persistence.Subscription {
	return p.subscriptionsStore
}

func (p *persistenceImpl) Session() persistence.Session {
	return p.sessionStore
}

func (p *persistenceImpl) Retained() persistence.Retained {
	return p.retainedStore
}

func (p *persistenceImpl) Queue(clientID string) persistence.Queue {
	if value, ok := p.queueStore.Load(clientID); ok {
		if val, ok := value.(*queue); ok {
			return val
		}
	}
	q := p.newQueue(clientID, p.cfg.MQTT.MaxQueuedMsg, time.Duration(p.cfg.MQTT.InflightExpiry))
	p.queueStore.Store(clientID, q)
	return q
}

func (p *persistenceImpl) Unack(clientID string) persistence.Unack {
	if value, ok := p.unackStore.Load(clientID); ok {
		if val, ok := value.(*unack); ok {
			return val
		}
	}
	u := p.newUnack(clientID)
	p.unackStore.Store(clientID, u)
	return u
}

func New(rdb redis.UniversalClient, cfg *config.Config, logger *slog.Logger) persistence.Persistence {
	impl := &persistenceImpl{
		rdb:    rdb,
		cfg:    cfg,
		logger: logger,
	}
	impl.sessionStore = impl.newSession()
	impl.subscriptionsStore = impl.newSubscription()
	impl.retainedStore = impl.newRetained()
	impl.willStore = impl.newWill()
	go func() {
		for message := range rdb.Subscribe(context.Background(), EventQueueChannel).Channel() {
			if value, ok := impl.queueStore.Load(message.Payload); ok {
				if val, ok := value.(*queue); ok {
					select {
					case val.waitLock <- struct{}{}:
					default:
						impl.logger.Debug("event queue missing", "node_msg", message.Payload)
					}
				}
			}
		}
	}()
	return impl
}
