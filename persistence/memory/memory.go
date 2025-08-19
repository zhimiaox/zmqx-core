package memory

import (
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/config"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"
)

type persistenceImpl struct {
	lockMap sync.Map

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
	// 清理session
	if err := p.sessionStore.Remove(clientID); err != nil {
		logger.Error("remove session", "err", err)
	}
	// 清理队列
	if value, ok := p.queueStore.LoadAndDelete(clientID); ok {
		if val, ok := value.(*queue); ok {
			if err := val.Clean(); err != nil {
				logger.Error("remove session queue", "err", err)
			}
		}
	}
	// 清理未应答msg id
	if value, ok := p.unackStore.LoadAndDelete(clientID); ok {
		if val, ok := value.(*unack); ok {
			if err := val.Clean(); err != nil {
				logger.Error("remove unack store", "err", err)
			}
		}
	}
	// 清理订阅
	if err := p.subscriptionsStore.UnsubscribeAll(clientID); err != nil {
		logger.Error("remove session unsubscribe all", "err", err)
	}
}

func (p *persistenceImpl) Unlock(clientID string) {
	p.lockMap.Delete(clientID)
}

func (p *persistenceImpl) TryLock(clientID string) bool {
	_, loaded := p.lockMap.LoadOrStore(clientID, struct{}{})
	return !loaded
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
	q := newQueue(clientID, p.cfg.MQTT.MaxQueuedMsg, time.Duration(p.cfg.MQTT.InflightExpiry))
	p.queueStore.Store(clientID, q)
	return q
}

func (p *persistenceImpl) Unack(clientID string) persistence.Unack {
	if value, ok := p.unackStore.Load(clientID); ok {
		if val, ok := value.(*unack); ok {
			return val
		}
	}
	u := newUnack()
	p.unackStore.Store(clientID, u)
	return u
}

func New(cfg *config.Config, logger *slog.Logger) persistence.Persistence {
	impl := &persistenceImpl{
		cfg:                cfg,
		sessionStore:       newSession(),
		subscriptionsStore: newSubscription(),
		retainedStore:      newRetained(),
		willStore:          newWill(),
		logger:             logger,
	}
	return impl
}
