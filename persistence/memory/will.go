package memory

import (
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Will = (*will)(nil)

type will struct {
	willMsg sync.Map
	tw      common.MemoryClock
	handler func(msg *models.WillMsg)
}

func (w *will) Event(handler func(msg *models.WillMsg)) {
	w.handler = handler
}

func (w *will) Add(msg *models.WillMsg) {
	w.willMsg.Store(msg.ClientID, msg)
	w.tw.Add(time.Now().Add(msg.After+time.Second), msg.ClientID)
}

func (w *will) Remove(clientID string) {
	w.willMsg.Delete(clientID)
}

func newWill() *will {
	w := &will{}
	w.tw = common.NewMemoryClock(func(data any) {
		if clientID, ok := data.(string); ok {
			if value, ok := w.willMsg.LoadAndDelete(clientID); ok {
				if msg, ok := value.(*models.WillMsg); ok {
					if w.handler != nil {
						w.handler(msg)
					}
				}
			}
		}
	})
	return w
}
