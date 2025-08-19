package models

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
)

// Session represents a MQTT session.
type Session struct {
	// ClientID represents the client id.
	ClientID string
	// Will is they will message of the client, can be nil if there is no will message.
	Will *Message
	// WillDelayInterval represents the Will Delay Interval in seconds
	WillDelayInterval uint32
	// ConnectedAt is the session create time.
	ConnectedAt time.Time
	// ExpiryInterval represents the Session Expiry Interval in seconds
	ExpiryInterval uint32
	// ExpiryAt 过期时间(链接关闭时设置 nil未关闭 zero不过期)
	ExpiryAt time.Time
}

// IsExpired return whether the session is expired
func (s *Session) IsExpired() bool {
	return !s.ExpiryAt.IsZero() && s.ExpiryAt.Before(time.Now())
}

func EncodeSession(ses *Session, b *bytes.Buffer) {
	common.WriteString(b, ses.ClientID)
	if ses.Will != nil {
		b.WriteByte(1)
		will := &bytes.Buffer{}
		EncodeMessage(ses.Will, will)
		common.WriteBytes(b, will.Bytes())
		common.WriteUint32(b, ses.WillDelayInterval)
	} else {
		b.WriteByte(0)
	}
	t := make([]byte, 8)
	binary.BigEndian.PutUint64(t, uint64(ses.ConnectedAt.Unix()))
	b.Write(t)
	common.WriteUint32(b, ses.ExpiryInterval)
	if ses.ExpiryAt.IsZero() {
		b.WriteByte(0)
	} else {
		b.WriteByte(1)
		binary.BigEndian.PutUint64(t, uint64(ses.ExpiryAt.Unix()))
		b.Write(t)
	}
}

func DecodeSession(b *bytes.Buffer) (*Session, error) {
	var err error
	ses := &Session{}
	ses.ClientID, err = common.ReadString(b)
	if err != nil {
		return nil, err
	}
	willPresent, err := b.ReadByte()
	if err != nil {
		return nil, err
	}
	if willPresent == 1 {
		will, err := common.ReadBytes(b)
		if err != nil {
			return nil, err
		}
		if ses.Will, err = DecodeMessage(bytes.NewBuffer(will)); err != nil {
			return nil, err
		}
		if ses.WillDelayInterval, err = common.ReadUint32(b); err != nil {
			return nil, err
		}
	}
	ses.ConnectedAt = time.Unix(int64(binary.BigEndian.Uint64(b.Next(8))), 0)
	ses.ExpiryInterval, err = common.ReadUint32(b)
	if err != nil {
		return nil, err
	}
	expiryPresent, err := b.ReadByte()
	if err != nil {
		return nil, err
	}
	if expiryPresent == 1 {
		ses.ExpiryAt = time.Unix(int64(binary.BigEndian.Uint64(b.Next(8))), 0)
	}
	return ses, nil
}
