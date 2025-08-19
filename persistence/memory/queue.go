package memory

import (
	"container/list"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Queue = (*queue)(nil)

type queue struct {
	clientID       string
	version        packets.Version
	maxQueueLen    int
	readBytesLimit uint32
	inflightExpiry time.Duration

	cond    *sync.Cond
	l       *list.List
	current *list.Element
	closed  bool

	onMsgDropped func(clientID string, msg *models.QueueElem, err error)
}

func newQueue(clientID string, maxQueuedMsg int, InflightExpiry time.Duration) *queue {
	return &queue{
		clientID:       clientID,
		maxQueueLen:    maxQueuedMsg,
		inflightExpiry: InflightExpiry,

		cond:         sync.NewCond(&sync.Mutex{}),
		l:            list.New(),
		onMsgDropped: func(clientID string, msg *models.QueueElem, err error) {},
	}
}

func (q *queue) Close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.closed = true
	q.cond.Signal()
}

func (q *queue) Init(opts *persistence.QueueInitOptions) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.readBytesLimit = opts.ReadBytesLimit
	q.version = opts.Version
	q.onMsgDropped = opts.OnMsgDropped
	q.closed = false
	if opts.CleanStart {
		q.l.Init()
	}
	q.current = q.l.Front()
	q.cond.Signal()
	return nil
}

func (q *queue) Clean() error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.l.Init()
	q.current = q.l.Front()
	return nil
}

func (q *queue) Add(elem *models.QueueElem) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.l.Len() >= q.maxQueueLen {
		q.onMsgDropped(q.clientID, elem, errors.QueueDropQueueFull)
		return errors.QueueDropQueueFull
	}
	e := q.l.PushBack(elem)
	if q.current == nil {
		q.current = e
	}
	q.cond.Signal()
	return nil
}

func (q *queue) ReadInflight(maxSize uint16) ([]*models.QueueElem, error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	rs := make([]*models.QueueElem, 0)
	for i := uint16(0); q.current != nil && i < maxSize; {
		elem, ok := q.current.Value.(*models.QueueElem)
		if !ok || elem.IsExpiry(time.Now()) {
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExpiredInflight)
			next := q.current.Next()
			q.l.Remove(q.current)
			q.current = next
			continue
		}
		if elem.ID() == 0 {
			break
		}
		rs = append(rs, elem)
		i++
		q.current = q.current.Next()
	}
	return rs, nil
}

func (q *queue) Read(packetIDs []packets.PacketID) ([]*models.QueueElem, error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.current == nil && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		return nil, errors.QueueClosed
	}
	rs := make([]*models.QueueElem, 0)
	pidUsed := 0
	for q.current != nil && pidUsed < len(packetIDs) {
		elem, ok := q.current.Value.(*models.QueueElem)
		// remove timeout element
		if !ok || elem.IsExpiry(time.Now()) {
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExpired)
			next := q.current.Next()
			q.l.Remove(q.current)
			q.current = next
			continue
		}
		pub, ok := elem.MessageWithID.(*models.Publish)
		// drop packets that exceed the limit
		if !ok || pub.TotalBytes(q.version) > q.readBytesLimit {
			q.onMsgDropped(q.clientID, elem, errors.QueueDropExceedsMaxPacketSize)
			next := q.current.Next()
			q.l.Remove(q.current)
			q.current = next
			continue
		}
		if pub.QoS > 0 {
			elem.SetID(packetIDs[pidUsed])
			pidUsed++

			elem.Expiry = time.Time{}
			// set the flight message expiration time
			if q.inflightExpiry != 0 {
				elem.Expiry = time.Now().Add(q.inflightExpiry)
			}

			q.current = q.current.Next()
		} else {
			next := q.current.Next()
			q.l.Remove(q.current)
			q.current = next
		}
		rs = append(rs, elem)
	}
	return rs, nil
}

func (q *queue) Replace(elem *models.QueueElem) (replaced bool, err error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for e := q.l.Front(); e != nil && e != q.current; e = e.Next() {
		if e.Value.(*models.QueueElem).ID() == elem.ID() {
			e.Value = elem
			return true, nil
		}
	}
	return false, nil
}

func (q *queue) Remove(pid packets.PacketID) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for e := q.l.Front(); e != nil && e != q.current; {
		if e.Value.(*models.QueueElem).ID() == pid {
			next := e.Next()
			q.l.Remove(e)
			e = next
			continue
		}
		e = e.Next()
	}
	return nil
}
