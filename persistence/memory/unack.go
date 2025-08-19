package memory

import (
	"sync"

	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Unack = (*unack)(nil)

type unack struct {
	data sync.Map
}

func newUnack() *unack {
	return &unack{
		data: sync.Map{},
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
	_, loaded := s.data.LoadOrStore(id, struct{}{})
	return loaded, nil
}

func (s *unack) Remove(id packets.PacketID) error {
	s.data.Delete(id)
	return nil
}

func (s *unack) Clean() error {
	s.data = sync.Map{}
	return nil
}
