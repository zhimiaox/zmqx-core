package redis

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Will = (*will)(nil)

type will struct {
	onceStart sync.Once
	tw        common.RedisClock
	rdb       redis.UniversalClient
	handler   func(msg *models.WillMsg)
	logger    *slog.Logger
}

func (w *will) Event(handler func(msg *models.WillMsg)) {
	w.handler = handler
	w.onceStart.Do(func() {
		go w.tw.Start()
	})
}

func (w *will) Add(msg *models.WillMsg) {
	data := &bytes.Buffer{}
	models.EncodeWillMsg(msg, data)
	w.rdb.HSet(context.Background(), willDataKey, msg.ClientID, data.String())
	if err := w.tw.Set(msg.ClientID, time.Now().Add(msg.After+time.Second)); err != nil {
		w.logger.Error("will msg clock add err", "err", err)
	}
}

func (w *will) Remove(clientID string) {
	_ = w.tw.Del(clientID)
	w.rdb.HDel(context.Background(), willDataKey, clientID)
}

func (w *will) callback(id string) {
	data := w.rdb.HGet(context.Background(), willDataKey, id).Val()
	if data != "" {
		w.rdb.HDel(context.Background(), willDataKey, id)
		if msg, err := models.DecodeWillMsg(bytes.NewBufferString(data)); err == nil && w.handler != nil {
			w.handler(msg)
		}
	}
}

func (p *persistenceImpl) newWill() *will {
	w := &will{rdb: p.rdb, logger: p.logger.With("persistence", "will")}
	w.tw = common.NewRedisClock(willClockKey, p.rdb, w.callback, w.logger)
	return w
}
