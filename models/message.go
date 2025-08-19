package models

import (
	"bytes"
	"errors"
	"io"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/packets"
)

type Message struct {
	Dup      bool
	QoS      uint8
	Retained bool
	Topic    string
	Payload  []byte
	PacketID packets.PacketID
	// The following fields are introduced in v5 specification.
	// Excepting MessageExpiry, these fields will not take effect when it represents a v3.x publish packet.
	ContentType            string
	CorrelationData        []byte
	MessageExpiry          uint32
	PayloadFormat          packets.PayloadFormat
	ResponseTopic          string
	SubscriptionIdentifier []uint32
	UserProperties         []packets.UserProperty
}

// Copy deep copies the Message and return the new one
func (m *Message) Copy() *Message {
	newMsg := &Message{
		Dup:           m.Dup,
		QoS:           m.QoS,
		Retained:      m.Retained,
		Topic:         m.Topic,
		PacketID:      m.PacketID,
		ContentType:   m.ContentType,
		MessageExpiry: m.MessageExpiry,
		PayloadFormat: m.PayloadFormat,
		ResponseTopic: m.ResponseTopic,
	}
	newMsg.Payload = make([]byte, len(m.Payload))
	copy(newMsg.Payload, m.Payload)

	if len(m.CorrelationData) != 0 {
		newMsg.CorrelationData = make([]byte, len(m.CorrelationData))
		copy(newMsg.CorrelationData, m.CorrelationData)
	}

	if len(m.SubscriptionIdentifier) != 0 {
		newMsg.SubscriptionIdentifier = make([]uint32, len(m.SubscriptionIdentifier))
		copy(newMsg.SubscriptionIdentifier, m.SubscriptionIdentifier)
	}
	if len(m.UserProperties) != 0 {
		newMsg.UserProperties = make([]packets.UserProperty, len(m.UserProperties))
		for k := range newMsg.UserProperties {
			newMsg.UserProperties[k].K = make([]byte, len(m.UserProperties[k].K))
			copy(newMsg.UserProperties[k].K, m.UserProperties[k].K)

			newMsg.UserProperties[k].V = make([]byte, len(m.UserProperties[k].V))
			copy(newMsg.UserProperties[k].V, m.UserProperties[k].V)
		}
	}
	return newMsg

}

func (m *Message) getVariableLen(l int) int {
	if l <= 127 {
		return 1
	} else if l <= 16383 {
		return 2
	} else if l <= 2097151 {
		return 3
	} else if l <= 268435455 {
		return 4
	}
	return 0
}

// TotalBytes return the publishing packets total bytes.
func (m *Message) TotalBytes(version packets.Version) uint32 {
	remainLength := len(m.Payload) + 2 + len(m.Topic)
	if m.QoS > packets.Qos0 {
		remainLength += 2
	}
	if packets.IsVersion5(version) {
		propertyLength := 0
		if m.PayloadFormat == packets.PayloadFormatString {
			propertyLength += 2
		}
		if l := len(m.ContentType); l != 0 {
			propertyLength += 3 + l
		}
		if l := len(m.CorrelationData); l != 0 {
			propertyLength += 3 + l
		}

		for _, v := range m.SubscriptionIdentifier {
			propertyLength++
			propertyLength += m.getVariableLen(int(v))
		}

		if m.MessageExpiry != 0 {
			propertyLength += 5
		}
		if l := len(m.ResponseTopic); l != 0 {
			propertyLength += 3 + l
		}
		for _, v := range m.UserProperties {
			propertyLength += 5 + len(v.K) + len(v.V)
		}
		remainLength += propertyLength + m.getVariableLen(propertyLength)
	}
	if remainLength <= 127 {
		return 2 + uint32(remainLength)
	} else if remainLength <= 16383 {
		return 3 + uint32(remainLength)
	} else if remainLength <= 2097151 {
		return 4 + uint32(remainLength)
	}
	return 5 + uint32(remainLength)
}

// MessageFromPublish create the Message instance from  publish packets
func MessageFromPublish(p *packets.Publish) *Message {
	m := &Message{
		Dup:      p.Dup,
		QoS:      p.Qos,
		Retained: p.Retain,
		Topic:    string(p.TopicName),
		Payload:  p.Payload,
	}
	if packets.IsVersion5(p.Version) {
		if p.Properties.PayloadFormat != nil {
			m.PayloadFormat = *p.Properties.PayloadFormat
		}
		if l := len(p.Properties.ContentType); l != 0 {
			m.ContentType = string(p.Properties.ContentType)
		}
		if l := len(p.Properties.CorrelationData); l != 0 {
			m.CorrelationData = p.Properties.CorrelationData
		}
		if p.Properties.MessageExpiry != nil {
			m.MessageExpiry = *p.Properties.MessageExpiry
		}
		if l := len(p.Properties.ResponseTopic); l != 0 {
			m.ResponseTopic = string(p.Properties.ResponseTopic)
		}
		m.UserProperties = p.Properties.User

	}
	return m
}

// MessageToPublish create the Publishing packet instance from *Message
func MessageToPublish(msg *Message, version packets.Version) *packets.Publish {
	pub := &packets.Publish{
		Dup:       msg.Dup,
		Qos:       msg.QoS,
		PacketID:  msg.PacketID,
		Retain:    msg.Retained,
		TopicName: []byte(msg.Topic),
		Payload:   msg.Payload,
		Version:   version,
	}
	if packets.IsVersion5(version) {
		var msgExpiry *uint32
		if e := msg.MessageExpiry; e != 0 {
			msgExpiry = &e
		}
		var contentType []byte
		if msg.ContentType != "" {
			contentType = []byte(msg.ContentType)
		}
		var responseTopic []byte
		if msg.ResponseTopic != "" {
			responseTopic = []byte(msg.ResponseTopic)
		}
		var payloadFormat *byte
		if e := msg.PayloadFormat; e == packets.PayloadFormatString {
			payloadFormat = &e
		}
		pub.Properties = &packets.Properties{
			CorrelationData:        msg.CorrelationData,
			ContentType:            contentType,
			MessageExpiry:          msgExpiry,
			ResponseTopic:          responseTopic,
			PayloadFormat:          payloadFormat,
			User:                   msg.UserProperties,
			SubscriptionIdentifier: msg.SubscriptionIdentifier,
		}
	}
	return pub
}

func WillMsgFromConnect(conn *packets.Connect) *Message {
	if !conn.WillFlag {
		return nil
	}
	willMsg := &Message{
		QoS:     conn.WillQos,
		Topic:   string(conn.WillTopic),
		Payload: conn.WillMsg,
	}
	if conn.WillProperties != nil {
		if conn.WillProperties.PayloadFormat != nil {
			willMsg.PayloadFormat = *conn.WillProperties.PayloadFormat
		}
		if conn.WillProperties.MessageExpiry != nil {
			willMsg.MessageExpiry = *conn.WillProperties.MessageExpiry
		}
		if conn.WillProperties.ContentType != nil {
			willMsg.ContentType = string(conn.WillProperties.ContentType)
		}
		if conn.WillProperties.ResponseTopic != nil {
			willMsg.ResponseTopic = string(conn.WillProperties.ResponseTopic)
		}
		if conn.WillProperties.CorrelationData != nil {
			willMsg.CorrelationData = conn.WillProperties.CorrelationData
		}
		willMsg.UserProperties = conn.WillProperties.User
	}
	return willMsg
}

// EncodeMessage encodes message into bytes and write it to the buffer
func EncodeMessage(msg *Message, b *bytes.Buffer) {
	if msg == nil {
		return
	}
	common.WriteBool(b, msg.Dup)
	b.WriteByte(msg.QoS)
	common.WriteBool(b, msg.Retained)
	common.WriteString(b, msg.Topic)
	common.WriteBytes(b, msg.Payload)
	common.WriteUint16(b, msg.PacketID)

	if len(msg.ContentType) != 0 {
		b.WriteByte(packets.PropContentType)
		common.WriteString(b, msg.ContentType)
	}
	if len(msg.CorrelationData) != 0 {
		b.WriteByte(packets.PropCorrelationData)
		common.WriteBytes(b, msg.CorrelationData)
	}
	if msg.MessageExpiry != 0 {
		b.WriteByte(packets.PropMessageExpiry)
		common.WriteUint32(b, msg.MessageExpiry)
	}
	b.WriteByte(packets.PropPayloadFormat)
	b.WriteByte(msg.PayloadFormat)

	if len(msg.ResponseTopic) != 0 {
		b.WriteByte(packets.PropResponseTopic)
		common.WriteString(b, msg.ResponseTopic)
	}
	for _, v := range msg.SubscriptionIdentifier {
		b.WriteByte(packets.PropSubscriptionIdentifier)
		l, _ := packets.DecodeRemainLength(int(v))
		b.Write(l)
	}
	for _, v := range msg.UserProperties {
		b.WriteByte(packets.PropUser)
		common.WriteBytes(b, v.K)
		common.WriteBytes(b, v.V)
	}
}

// DecodeMessage decodes message from buffer.
func DecodeMessage(b *bytes.Buffer) (msg *Message, err error) {
	msg = &Message{}
	if msg.Dup, err = common.ReadBool(b); err != nil {
		return
	}
	if msg.QoS, err = b.ReadByte(); err != nil {
		return
	}
	if msg.Retained, err = common.ReadBool(b); err != nil {
		return
	}
	if msg.Topic, err = common.ReadString(b); err != nil {
		return
	}
	if msg.Payload, err = common.ReadBytes(b); err != nil {
		return
	}
	if msg.PacketID, err = common.ReadUint16(b); err != nil {
		return
	}
	for {
		pt, err := b.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return msg, nil
			}
			return nil, err
		}
		switch pt {
		case packets.PropContentType:
			if msg.ContentType, err = common.ReadString(b); err != nil {
				return nil, err
			}
		case packets.PropCorrelationData:
			if msg.CorrelationData, err = common.ReadBytes(b); err != nil {
				return nil, err
			}
		case packets.PropMessageExpiry:
			if msg.MessageExpiry, err = common.ReadUint32(b); err != nil {
				return nil, err
			}
		case packets.PropPayloadFormat:
			if msg.PayloadFormat, err = b.ReadByte(); err != nil {
				return nil, err
			}
		case packets.PropResponseTopic:
			if msg.ResponseTopic, err = common.ReadString(b); err != nil {
				return nil, err
			}
		case packets.PropSubscriptionIdentifier:
			si, err := packets.EncodeRemainLength(b)
			if err != nil {
				return nil, err
			}
			msg.SubscriptionIdentifier = append(msg.SubscriptionIdentifier, uint32(si))
		case packets.PropUser:
			k, err := common.ReadBytes(b)
			if err != nil {
				return nil, err
			}
			v, err := common.ReadBytes(b)
			if err != nil {
				return nil, err
			}
			msg.UserProperties = append(msg.UserProperties, packets.UserProperty{K: k, V: v})
		}
	}
}
