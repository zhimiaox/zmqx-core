package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Queue = (*queue)(nil)

type queue struct {
	rdb            redis.UniversalClient
	clientID       string
	version        packets.Version
	readBytesLimit uint32
	// maxQueueLen is the maximum queue length
	maxQueueLen int

	currentKey string

	inflightKey    string
	inflightMu     sync.RWMutex
	inflightIdx    int64
	inflightCache  map[packets.PacketID]string
	inflightExpiry time.Duration

	waitLock chan struct{}
	ctx      struct {
		context.Context
		cancel context.CancelFunc
	}
	onMsgDropped func(clientID string, msg *models.QueueElem, err error)
	logger       *slog.Logger
}

func (p *persistenceImpl) newQueue(clientID string, maxQueuedMsg int, InflightExpiry time.Duration) *queue {
	return &queue{
		clientID:       clientID,
		currentKey:     fmt.Sprintf(queueCurrentKey, clientID),
		inflightKey:    fmt.Sprintf(queueInflightKey, clientID),
		rdb:            p.rdb,
		maxQueueLen:    maxQueuedMsg,
		inflightExpiry: InflightExpiry,
		waitLock:       make(chan struct{}, 1),
		onMsgDropped:   func(clientID string, msg *models.QueueElem, err error) {},
		logger:         p.logger.With("persistence", "queue", "client_id", clientID),
	}
}

func (q *queue) Close() {
	q.ctx.cancel()
}

func (q *queue) Init(opts *persistence.QueueInitOptions) error {
	q.readBytesLimit = opts.ReadBytesLimit
	q.version = opts.Version
	if opts.CleanStart {
		if err := q.Clean(); err != nil {
			return err
		}
	}
	q.inflightIdx = 0
	q.inflightCache = make(map[packets.PacketID]string)
	q.onMsgDropped = opts.OnMsgDropped
	q.ctx.Context, q.ctx.cancel = context.WithCancel(context.Background())
	return nil
}

func (q *queue) Clean() error {
	return q.rdb.Del(context.Background(), q.currentKey, q.inflightKey).Err()
}

func (q *queue) Add(elem *models.QueueElem) error {
	ctx := context.Background()
	if q.maxQueueLen > 0 && q.rdb.LLen(ctx, q.currentKey).Val() > int64(q.maxQueueLen) {
		q.onMsgDropped(q.clientID, elem, errors.QueueDropQueueFull)
		return nil
	}
	if err := q.rdb.RPush(ctx, q.currentKey, string(elem.Encode())).Err(); err != nil {
		q.onMsgDropped(q.clientID, elem, err)
		return err
	}
	// TODO: 当前采用redis订阅通知，速度还行，但终归是有延迟
	q.rdb.Publish(ctx, EventQueueChannel, q.clientID)
	return nil
}

func (q *queue) Replace(elem *models.QueueElem) (replaced bool, err error) {
	q.inflightMu.Lock()
	defer q.inflightMu.Unlock()
	ctx := context.Background()
	flights, err := q.rdb.LRange(ctx, q.inflightKey, 0, -1).Result()
	if err != nil {
		return false, err
	}
	for i := range flights {
		e := new(models.QueueElem)
		if err = e.Decode([]byte(flights[i])); err != nil {
			q.logger.Error("飞行消息解码失败", "err", err)
			if err = q.rdb.LRem(ctx, q.inflightKey, 0, flights[i]).Err(); err != nil {
				return false, err
			}
			continue
		}
		if e.ID() == elem.ID() {
			q.inflightCache[elem.ID()] = string(elem.Encode())
			if err = q.rdb.LSet(ctx, q.inflightKey, int64(i), q.inflightCache[elem.ID()]).Err(); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (q *queue) Read(packetIDs []packets.PacketID) ([]*models.QueueElem, error) {
	ctx := context.Background()
	rs := make([]*models.QueueElem, 0)
	isFirst := true
	used := 0
	for maxNum := 0; maxNum < len(packetIDs); maxNum++ {
		data, err := q.rdb.LPop(ctx, q.currentKey).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		if data == "" {
			if isFirst {
				select {
				case <-q.waitLock:
				case <-q.ctx.Done():
					return nil, errors.QueueClosed
				// 兜底操作，防止消息彻底堵死
				case <-time.After(time.Minute):
				}
			} else {
				break
			}
		}
		isFirst = false
		elem := new(models.QueueElem)
		if err := elem.Decode([]byte(data)); err != nil || elem.IsExpiry(time.Now()) {
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExpired)
			continue
		}
		pub, ok := elem.MessageWithID.(*models.Publish)
		if !ok || pub.TotalBytes(q.version) > q.readBytesLimit {
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExceedsMaxPacketSize)
			continue
		}
		if pub.QoS != 0 {
			elem.SetID(packetIDs[used])
			used++
			// set flight message expiration time
			if q.inflightExpiry != 0 {
				elem.Expiry = time.Now().Add(q.inflightExpiry)
			} else {
				elem.Expiry = time.Time{}
			}
			elemData := string(elem.Encode())
			if err = q.rdb.RPush(ctx, q.inflightKey, elemData).Err(); err != nil {
				return nil, err
			}
			q.inflightMu.Lock()
			q.inflightCache[elem.ID()] = elemData
			q.inflightMu.Unlock()
		}
		rs = append(rs, elem)
	}
	return rs, nil
}

func (q *queue) ReadInflight(maxSize uint16) ([]*models.QueueElem, error) {
	q.inflightMu.Lock()
	defer q.inflightMu.Unlock()
	ctx := context.Background()
	flights, err := q.rdb.LRange(ctx, q.inflightKey, q.inflightIdx, q.inflightIdx+int64(maxSize)-1).Result()
	if err != nil || len(flights) == 0 {
		return nil, err
	}
	rs := make([]*models.QueueElem, 0)
	for i := range flights {
		elem := new(models.QueueElem)
		if err = elem.Decode([]byte(flights[i])); err != nil {
			if err = q.rdb.LRem(ctx, q.inflightKey, 0, flights[i]).Err(); err != nil {
				return nil, err
			}
			continue
		}
		if elem.IsExpiry(time.Now()) {
			if err = q.rdb.LRem(ctx, q.inflightKey, 0, flights[i]).Err(); err != nil {
				return nil, err
			}
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExpiredInflight)
			continue
		}
		rs = append(rs, elem)
		q.inflightCache[elem.ID()] = flights[i]
		q.inflightIdx++
	}
	if len(rs) == 0 {
		return q.ReadInflight(maxSize)
	}
	return rs, nil
}

func (q *queue) Remove(pid packets.PacketID) error {
	q.inflightMu.Lock()
	defer q.inflightMu.Unlock()
	if b, ok := q.inflightCache[pid]; ok {
		delete(q.inflightCache, pid)
		if err := q.rdb.LRem(context.Background(), q.inflightKey, 0, b).Err(); err != nil {
			return err
		}
	}
	return nil
}
