package server

import (
	"math/rand"
	"time"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
)

// deliver controllers the delivery behaviors according to the DeliveryMode config. (overlap or onlyonce)
type deliver struct {
	fn      models.SubscriptionIterateFn
	sl      sharedList
	mq      maxQos
	matched bool
	now     time.Time
	msg     *models.Message
	srv     *server
}

// sharedList is the subscriber (client id) list of shared subscriptions. (key by topic name).
type sharedList map[string][]struct {
	clientID string
	sub      *models.Subscription
}

// maxQos records the maximum qos subscription for the non-shared topic. (key by topic name).
type maxQos map[string]*struct {
	sub    *models.Subscription
	subIDs []uint32
}

func (srv *server) deliver(srcClientID string, msg *models.Message) (matched bool) {
	d := &deliver{
		sl:  make(sharedList),
		mq:  make(maxQos),
		msg: msg,
		srv: srv,
		now: time.Now(),
	}
	var iterateFn models.SubscriptionIterateFn
	d.fn = func(clientID string, sub *models.Subscription) bool {
		if sub.NoLocal && clientID == srcClientID {
			return true
		}
		d.matched = true
		if sub.ShareName != "" {
			fullTopic := sub.GetFullTopicName()
			d.sl[fullTopic] = append(d.sl[fullTopic], struct {
				clientID string
				sub      *models.Subscription
			}{clientID: clientID, sub: sub})
			return true
		}
		return iterateFn(clientID, sub)
	}
	if srv.cfg.MQTT.DeliveryMode == consts.Overlap {
		iterateFn = func(clientID string, sub *models.Subscription) bool {
			d.addMsgToQueue(clientID, msg.Copy(), sub, []uint32{sub.ID})
			return true
		}
	} else {
		iterateFn = func(clientID string, sub *models.Subscription) bool {
			// If the delivery mode is onlyOnce, set the message qos to the maximum qos in matched subscriptions.
			if d.mq[clientID] == nil {
				d.mq[clientID] = &struct {
					sub    *models.Subscription
					subIDs []uint32
				}{sub: sub, subIDs: []uint32{sub.ID}}
				return true
			}
			if d.mq[clientID].sub.QoS < sub.QoS {
				d.mq[clientID].sub = sub
			}
			d.mq[clientID].subIDs = append(d.mq[clientID].subIDs, sub.ID)
			return true
		}
	}
	srv.persistence.Subscription().Iterate(d.fn, models.IterationOptions{
		Type:      consts.TypeAll,
		ClientID:  "",
		TopicName: msg.Topic,
		MatchType: consts.MatchFilter,
	})
	// shared subscription
	for _, v := range d.sl {
		// 共享订阅消息，同主题随机挑一个节点扔过去
		// TODO enable customize balance strategy of shared subscription
		rs := v[rand.Intn(len(v))]
		d.addMsgToQueue(rs.clientID, d.msg.Copy(), rs.sub, []uint32{rs.sub.ID})
	}
	// For onlyonce mode, send the non-shared messages.
	for clientID, v := range d.mq {
		d.addMsgToQueue(clientID, d.msg.Copy(), v.sub, v.subIDs)
	}
	return d.matched
}

func (d *deliver) addMsgToQueue(clientID string, msg *models.Message, sub *models.Subscription, ids []uint32) {
	d.srv.logger.Debug("消息入列",
		"client_id", clientID,
		"msg", string(msg.Payload[:min(100, len(msg.Payload))]),
		"msg_topic", msg.Topic,
		"msg_qos", msg.QoS,
		"sub_topic", sub.TopicFilter,
		"sub_qos", sub.QoS)
	mqttCfg := &d.srv.cfg.MQTT
	// If the client with the clientID is not connected, skip qos0 messages.
	if msg.QoS == packets.Qos0 && !mqttCfg.QueueQos0Msg {
		d.srv.clientsMu.RLock()
		_, ok := d.srv.clients[clientID]
		d.srv.clientsMu.RUnlock()
		if !ok {
			return
		}
	}
	if msg.QoS > sub.QoS {
		msg.QoS = sub.QoS
	}
	for _, id := range ids {
		if id != 0 {
			msg.SubscriptionIdentifier = append(msg.SubscriptionIdentifier, id)
		}
	}
	msg.Dup = false
	if !sub.RetainAsPublished {
		msg.Retained = false
	}
	var expiry time.Time
	if mqttCfg.MessageExpiry != 0 {
		if msg.MessageExpiry != 0 && int(msg.MessageExpiry) <= int(mqttCfg.MessageExpiry) {
			expiry = d.now.Add(time.Duration(msg.MessageExpiry) * time.Second)
		} else {
			expiry = d.now.Add(time.Duration(mqttCfg.MessageExpiry))
		}
	} else if msg.MessageExpiry != 0 {
		expiry = d.now.Add(time.Duration(msg.MessageExpiry) * time.Second)
	}
	if err := d.srv.persistence.Queue(clientID).Add(&models.QueueElem{
		At:     d.now,
		Expiry: expiry,
		MessageWithID: &models.Publish{
			Message: msg,
		},
	}); err != nil {
		d.srv.logger.Warn("message dropped", "client_id", clientID, "err", err)
		return
	}
}
