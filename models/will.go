package models

import (
	"bytes"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
)

// WillMsg 遗嘱消息
type WillMsg struct {
	ClientID string
	After    time.Duration
	Msg      *Message
}

func EncodeWillMsg(will *WillMsg, b *bytes.Buffer) {
	common.WriteString(b, will.ClientID)
	common.WriteUint32(b, uint32(will.After.Seconds()))
	EncodeMessage(will.Msg, b)
}

func DecodeWillMsg(b *bytes.Buffer) (*WillMsg, error) {
	will := &WillMsg{}
	var err error
	if will.ClientID, err = common.ReadString(b); err != nil {
		return nil, err
	}
	afterSecond, err := common.ReadUint32(b)
	if err != nil {
		return nil, err
	}
	will.After = time.Second * time.Duration(afterSecond)
	will.Msg, err = DecodeMessage(b)
	if err != nil {
		return nil, err
	}
	return will, nil
}
