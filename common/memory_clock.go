package common

import (
	"sync"
	"time"
)

type MemoryClock interface {
	Add(t time.Time, data any)
	Close()
}

type memoryClockData struct {
	clock time.Time
	data  any
}

type memoryClockImpl struct {
	done  chan struct{}
	index uint32
	tv1   [256][]*memoryClockData // 0000 0000 0000 0000 0000 0000 1111 1111
	tv2   [64][]*memoryClockData  // 0000 0000 0000 0000 0011 1111 0000 0000
	tv3   [64][]*memoryClockData  // 0000 0000 0000 1111 1100 0000 0000 0000
	tv4   [64][]*memoryClockData  // 0000 0011 1111 0000 0000 0000 0000 0000
	tv5   [64][]*memoryClockData  // 1111 1100 0000 0000 0000 0000 0000 0000
	sync.RWMutex
	callback func(data any)
}

func (tw *memoryClockImpl) Close() {
	close(tw.done)
}

func (tw *memoryClockImpl) Add(t time.Time, data any) {
	tw.add(&memoryClockData{
		clock: t,
		data:  data,
	})
}

func (tw *memoryClockImpl) add(item *memoryClockData) {
	diff := time.Until(item.clock).Seconds()
	if diff < 1 {
		go tw.callback(item.data)
		return
	}
	after := uint32(diff)
	// 一轮
	after += tw.index & 0xFF
	if after < (1 << 8) {
		if tw.tv1[after] == nil {
			tw.tv1[after] = make([]*memoryClockData, 0)
		}
		tw.tv1[after] = append(tw.tv1[after], item)
		return
	}
	// 二轮
	after += tw.index & (0x3F << 8)
	if after < (1 << 14) {
		lv := (after >> 8) & 0x3F
		if tw.tv2[lv] == nil {
			tw.tv2[lv] = make([]*memoryClockData, 0)
		}
		tw.tv2[lv] = append(tw.tv2[lv], item)
		return
	}
	// 三轮
	after += tw.index & (0x3F << 14)
	if after < (1 << 20) {
		lv := (after >> 14) & 0x3F
		if tw.tv3[lv] == nil {
			tw.tv3[lv] = make([]*memoryClockData, 0)
		}
		tw.tv3[lv] = append(tw.tv3[lv], item)
		return
	}
	// 四轮
	after += tw.index & (0x3F << 20)
	if after < (1 << 26) {
		lv := (after >> 20) & 0x3F
		if tw.tv4[lv] == nil {
			tw.tv4[lv] = make([]*memoryClockData, 0)
		}
		tw.tv4[lv] = append(tw.tv4[lv], item)
		return
	}
	// 五轮
	after += tw.index & (0x3F << 26)
	lv := after >> 26
	if tw.tv5[lv] == nil {
		tw.tv5[lv] = make([]*memoryClockData, 0)
	}
	tw.tv5[lv] = append(tw.tv5[lv], item)
}

func (tw *memoryClockImpl) tick() {
	tw.index++
	// 一轮跳帧
	idx := tw.index & 0xFF
	expireData := tw.tv1[idx]
	tw.tv1[idx] = nil
	for _, v := range expireData {
		go tw.callback(v.data)
	}
	// 二轮跳帧
	if idx != 0 {
		return
	}
	idx = (tw.index >> 8) & 0x3F
	expireData = tw.tv2[idx]
	tw.tv2[idx] = nil
	for _, v := range expireData {
		tw.add(v)
	}
	// 三轮跳帧
	if idx != 0 {
		return
	}
	idx = (tw.index >> 14) & 0x3F
	expireData = tw.tv3[idx]
	tw.tv3[idx] = nil
	for _, v := range expireData {
		tw.add(v)
	}
	// 四轮跳帧
	if idx != 0 {
		return
	}
	idx = (tw.index >> 20) & 0x3F
	expireData = tw.tv4[idx]
	tw.tv4[idx] = nil
	for _, v := range expireData {
		tw.add(v)
	}
	// 五轮跳帧
	if idx != 0 {
		return
	}
	idx = tw.index >> 26
	expireData = tw.tv5[idx]
	tw.tv5[idx] = nil
	for _, v := range expireData {
		tw.add(v)
	}
}

func NewMemoryClock(callback func(data any)) MemoryClock {
	impl := &memoryClockImpl{
		done:     make(chan struct{}),
		callback: callback,
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				impl.tick()
			case <-impl.done:
				return
			}
		}
	}()
	return impl
}
