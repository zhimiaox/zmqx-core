package memory

import (
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Session = (*session)(nil)

type session struct {
	data    sync.Map
	tw      common.MemoryClock
	handler func(clientID string)
}

func newSession() persistence.Session {
	impl := &session{}
	impl.tw = common.NewMemoryClock(func(data any) {
		if clientID, ok := data.(string); ok && impl.handler != nil {
			if _, ok = impl.data.Load(clientID); ok {
				impl.handler(clientID)
			}
		}
	})
	return impl
}
func (s *session) Event(handler func(clientID string)) {
	s.handler = handler
}

func (s *session) Set(session *models.Session) error {
	s.data.Store(session.ClientID, session)
	if !session.ExpiryAt.IsZero() {
		s.tw.Add(session.ExpiryAt.Add(time.Second), session.ClientID)
	}
	return nil
}

func (s *session) Remove(clientID string) error {
	s.data.Delete(clientID)
	return nil
}

func (s *session) Get(clientID string) (*models.Session, error) {
	if value, ok := s.data.Load(clientID); ok {
		if ses, ok := value.(*models.Session); ok {
			return ses, nil
		}
	}
	return nil, nil
}
