package models

import (
	"bytes"
	"errors"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/packets"
)

// SubscriptionIterateFn is the callback function used by iterate()
// Return false means to stop the iteration.
type SubscriptionIterateFn func(clientID string, sub *Subscription) bool

// SubscribeResult is the result of Subscribe()
type SubscribeResult struct {
	// Topic is the Subscribed topic
	Subscription *Subscription
	// AlreadyExisted shows whether the topic is already existed.
	AlreadyExisted bool
}

// ClientSubscriptions groups the subscriptions by client id.
type ClientSubscriptions map[string][]*Subscription

// ClientSubscription subscriptions by client id.
type ClientSubscription map[string]*Subscription

// IterationOptions subscription iterated options
type IterationOptions struct {
	// Type specifies the types of subscription that will be iterated.
	// For example, if Type = TypeShared | TypeNonShared , then all shared and non-shared subscriptions will be iterated
	Type consts.IterationType
	// ClientID specifies the subscriber client id.
	ClientID string
	// TopicName represents topic filter or topic name. This field works together with MatchType.
	TopicName string
	// MatchType specifies the matching type of the iteration.
	// if MatchName, the SubscriptionIterateFn will be called when the subscription topic filter is equal to TopicName.
	// if MatchTopic,  the SubscriptionIterateFn will be called when the TopicName match the subscription topic filter.
	MatchType consts.MatchType
}

// Subscription represents a subscription in broker.
type Subscription struct {
	// ShareName is the share name of a shared subscription.
	// set to "" if it is a non-shared subscription.
	ShareName string
	// TopicFilter is the topic filter which does not include the share name.
	TopicFilter string
	// ID is the subscription identifier
	ID uint32
	// The following fields are Subscription Options.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169

	// QoS is the qos level of the Subscription.
	QoS packets.QoS
	// NoLocal is the No Local option. 值为1，表示应用消息不能被转发给发布此消息的客户标识符
	NoLocal bool
	// RetainAsPublished is the Retain As Published option.
	RetainAsPublished bool
	// RetainHandling the Retain Handling option.
	RetainHandling byte
}

// FromTopic returns the subscription instance for given topic and subscription id.
func (s *Subscription) FromTopic(topic packets.Topic, id uint32) *Subscription {
	s.ShareName, s.TopicFilter = common.SplitTopic(topic.Name)
	s.ID = id
	s.QoS = topic.Qos
	s.NoLocal = topic.NoLocal
	s.RetainAsPublished = topic.RetainAsPublished
	s.RetainHandling = topic.RetainHandling
	return s
}

// GetFullTopicName returns the full topic name of the subscription.
func (s *Subscription) GetFullTopicName() string {
	if s.ShareName != "" {
		return "$share/" + s.ShareName + "/" + s.TopicFilter
	}
	return s.TopicFilter
}

// Copy makes a copy of subscription.
func (s *Subscription) Copy() *Subscription {
	return &Subscription{
		ShareName:         s.ShareName,
		TopicFilter:       s.TopicFilter,
		ID:                s.ID,
		QoS:               s.QoS,
		NoLocal:           s.NoLocal,
		RetainAsPublished: s.RetainAsPublished,
		RetainHandling:    s.RetainHandling,
	}
}

// Validate returns whether the subscription is valid.
// If you can ensure the subscription is valid then just skip the validation.
func (s *Subscription) Validate() error {
	if !packets.ValidV5Topic([]byte(s.GetFullTopicName())) {
		return errors.New("invalid topic name")
	}
	if s.QoS > 2 {
		return errors.New("invalid qos")
	}
	if s.RetainHandling != 0 && s.RetainHandling != 1 && s.RetainHandling != 2 {
		return errors.New("invalid retain handling")
	}
	return nil
}

func EncodeSubscription(sub *Subscription, buf *bytes.Buffer) {
	common.WriteString(buf, sub.ShareName)
	common.WriteString(buf, sub.TopicFilter)
	common.WriteUint32(buf, sub.ID)
	buf.WriteByte(sub.QoS)
	common.WriteBool(buf, sub.NoLocal)
	common.WriteBool(buf, sub.RetainAsPublished)
	buf.WriteByte(sub.RetainHandling)
}

func DecodeSubscription(buf *bytes.Buffer) (*Subscription, error) {
	var err error
	sub := &Subscription{}
	if sub.ShareName, err = common.ReadString(buf); err != nil {
		return nil, err
	}
	if sub.TopicFilter, err = common.ReadString(buf); err != nil {
		return nil, err
	}
	if sub.ID, err = common.ReadUint32(buf); err != nil {
		return nil, err
	}
	if sub.QoS, err = buf.ReadByte(); err != nil {
		return nil, err
	}
	if sub.NoLocal, err = common.ReadBool(buf); err != nil {
		return nil, err
	}
	if sub.RetainAsPublished, err = common.ReadBool(buf); err != nil {
		return nil, err
	}
	if sub.RetainHandling, err = buf.ReadByte(); err != nil {
		return nil, err
	}
	return sub, nil
}
