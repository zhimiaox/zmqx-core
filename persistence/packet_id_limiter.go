package persistence

import (
	"sync"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/packets"
)

// PacketIDLimiter limit the generation of packet id to keep the number of inflight messages
// always less or equal than receive maximum setting of the client.
type PacketIDLimiter interface {
	// PollPacketIDs returns at most max number of unused packetID and marks them as used for a client.
	// If there is no available id, the call will be blocked until at least one packet id is available or the limiter has been closed.
	// return 0 means the limiter is closed.
	// the return number = min(max, i.used).
	PollPacketIDs(max uint16) []packets.PacketID
	// Release marks the given id list as unused
	Release(id packets.PacketID)
	ReleaseLocked(id packets.PacketID)
	BatchRelease(id []packets.PacketID)
	// MarkUsedLocked marks the given id as used.
	MarkUsedLocked(id packets.PacketID)
	Lock()
	Unlock()
	UnlockAndSignal()
	Close()
}

type packetIdLimiterImpl struct {
	cond      *sync.Cond
	used      uint16
	limit     uint16
	exit      bool
	lockedPid *common.Bitmap   // packet id in-use
	freePid   packets.PacketID // next available id
}

func NewPacketIDLimiter(limit uint16) PacketIDLimiter {
	return &packetIdLimiterImpl{
		cond:      sync.NewCond(&sync.Mutex{}),
		used:      0,
		limit:     limit,
		exit:      false,
		freePid:   1,
		lockedPid: common.NewBitmap(packets.MaxPacketID),
	}
}

func (p *packetIdLimiterImpl) Close() {
	p.cond.L.Lock()
	p.exit = true
	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *packetIdLimiterImpl) PollPacketIDs(max uint16) []packets.PacketID {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()
	for p.used >= p.limit && !p.exit {
		p.cond.Wait()
	}
	if p.exit {
		return nil
	}
	n := max
	if remain := p.limit - p.used; remain < max {
		n = remain
	}
	id := make([]packets.PacketID, 0)
	for j := uint16(0); j < n; j++ {
		for p.lockedPid.Get(p.freePid) {
			if p.freePid == packets.MaxPacketID {
				p.freePid = packets.MinPacketID
			} else {
				p.freePid++
			}
		}
		id = append(id, p.freePid)
		p.used++
		p.lockedPid.Set(p.freePid, true)
		if p.freePid == packets.MaxPacketID {
			p.freePid = packets.MinPacketID
		} else {
			p.freePid++
		}
	}
	return id
}

func (p *packetIdLimiterImpl) Release(id packets.PacketID) {
	p.cond.L.Lock()
	p.ReleaseLocked(id)
	p.cond.L.Unlock()
	p.cond.Signal()

}
func (p *packetIdLimiterImpl) ReleaseLocked(id packets.PacketID) {
	if p.lockedPid.Get(id) {
		p.lockedPid.Set(id, false)
		p.used--
	}
}

func (p *packetIdLimiterImpl) BatchRelease(id []packets.PacketID) {
	p.cond.L.Lock()
	for _, v := range id {
		p.ReleaseLocked(v)
	}
	p.cond.L.Unlock()
	p.cond.Signal()

}

func (p *packetIdLimiterImpl) MarkUsedLocked(id packets.PacketID) {
	p.used++
	p.lockedPid.Set(id, true)
}

func (p *packetIdLimiterImpl) Lock() {
	p.cond.L.Lock()
}
func (p *packetIdLimiterImpl) Unlock() {
	p.cond.L.Unlock()
}
func (p *packetIdLimiterImpl) UnlockAndSignal() {
	p.cond.L.Unlock()
	p.cond.Signal()
}
