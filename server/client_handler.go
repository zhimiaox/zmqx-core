package server

import (
	"bytes"
	"context"
	"sync"

	"math"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
)

func (c *client) readHandle(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case packet := <-c.in:
			if packets.IsVersion5(c.version) && c.opts.ServerMaxPacketSize != 0 &&
				packets.TotalBytes(packet) > c.opts.ServerMaxPacketSize {
				c.Error(errors.NewError(consts.PacketTooLarge))
				continue
			}
			var codeErr *errors.Error
			switch p := packet.(type) {
			case *packets.Subscribe:
				codeErr = c.subscribeHandler(p)
			case *packets.Publish:
				codeErr = c.publishHandler(p)
			case *packets.Puback:
				codeErr = c.pubackHandler(p)
			case *packets.Pubrel:
				codeErr = c.pubrelHandler(p)
			case *packets.Pubrec:
				c.pubrecHandler(p)
			case *packets.Pubcomp:
				c.pubcompHandler(p)
			case *packets.Pingreq:
				c.pingreqHandler(p)
			case *packets.Unsubscribe:
				c.unsubscribeHandler(p)
			case *packets.Disconnect:
				codeErr = c.disconnectHandler(p)
				c.ctx.cancel() // 关闭客户端
			case *packets.Auth:
				if packets.IsVersion3X(c.version) || !bytes.Equal(c.opts.AuthMethod, p.Properties.AuthMethod) {
					codeErr = errors.ErrProtocol
					break
				}
				codeErr = c.reAuthHandler(p)
			default:
				codeErr = errors.ErrProtocol
			}
			if codeErr != nil {
				c.Error(codeErr)
				return
			}
		}
	}
}

func (c *client) connectHandler(ctx context.Context, conn *packets.Connect) *packets.Connack {
	conf := &c.srv.cfg.MQTT
	connack := conn.NewConnackPacket(consts.Success)
	if !conf.AllowZeroLenClientID && len(conn.ClientID) == 0 {
		connack.Code = consts.ClientIdentifierNotValid
		return connack
	}
	authOpts := &models.AuthOptions{
		SessionExpiry:        uint32(time.Duration(conf.SessionExpiry).Seconds()),
		ReceiveMax:           conf.ReceiveMax,
		MaximumQoS:           conf.MaximumQoS,
		MaxPacketSize:        conf.MaxPacketSize,
		TopicAliasMax:        conf.TopicAliasMax,
		RetainAvailable:      conf.RetainAvailable,
		WildcardSubAvailable: conf.WildcardAvailable,
		SubIDAvailable:       conf.SubscriptionIDAvailable,
		SharedSubAvailable:   conf.SharedSubAvailable,
		KeepAlive:            min(conn.KeepAlive, conf.MaxKeepAlive),
		MaxInflight:          conf.MaxInflight,
	}
	isBasicAuth := true

	if packets.IsVersion5(conn.Version) {
		if conn.Properties.SessionExpiryInterval != nil {
			authOpts.SessionExpiry = min(*conn.Properties.SessionExpiryInterval, authOpts.SessionExpiry)
		} else {
			authOpts.SessionExpiry = 0
		}
		if conn.Properties.AuthMethod != nil {
			resp, err := c.authHandler(ctx, conn, authOpts)
			if err != nil {
				connack.Code = consts.BadAuthMethod
				return connack
			}
			if connack.Properties == nil {
				connack.Properties = new(packets.Properties)
			}
			connack.Properties.AuthMethod = conn.Properties.AuthMethod
			connack.Properties.AuthData = resp.AuthData
			isBasicAuth = false
		}
	}
	if isBasicAuth {
		if err := c.srv.hooks.OnBasicAuth(conn, authOpts); err != nil {
			connack.Code = consts.BadUserNameOrPassword
			return connack
		}
	}
	// authentication success
	c.cleanStart = conn.CleanStart
	c.version = conn.Version
	c.opts.RetainAvailable = authOpts.RetainAvailable
	c.opts.WildcardSubAvailable = authOpts.WildcardSubAvailable
	c.opts.SubIDAvailable = authOpts.SubIDAvailable
	c.opts.SharedSubAvailable = authOpts.SharedSubAvailable
	c.opts.SessionExpiry = authOpts.SessionExpiry
	c.opts.MaxInflight = authOpts.MaxInflight
	c.opts.ReceiveMax = authOpts.ReceiveMax
	c.opts.ClientMaxPacketSize = math.MaxUint32 // unlimited
	c.opts.ServerMaxPacketSize = authOpts.MaxPacketSize
	c.opts.ServerTopicAliasMax = authOpts.TopicAliasMax // 服务端侧最大接受alias num
	c.opts.KeepAlive = conn.KeepAlive
	c.opts.Username = string(conn.Username)
	c.opts.ClientID = string(conn.ClientID)
	if c.opts.ClientID == "" {
		if len(authOpts.AssignedClientID) != 0 {
			c.opts.ClientID = string(authOpts.AssignedClientID)
		} else {
			c.opts.ClientID = common.NanoID()
			authOpts.AssignedClientID = []byte(c.opts.ClientID)
		}
	}
	if packets.IsVersion5(c.version) {
		c.opts.MaxInflight = common.ConvertPtr(conn.Properties.ReceiveMaximum, c.opts.MaxInflight)
		c.opts.ClientMaxPacketSize = common.ConvertPtr(conn.Properties.MaximumPacketSize, c.opts.ClientMaxPacketSize)
		c.opts.ClientTopicAliasMax = common.ConvertPtr(conn.Properties.TopicAliasMaximum, 0) // 客户端侧最大接受alias max num
		c.opts.AuthMethod = conn.Properties.AuthMethod
		c.serverReceiveMaximumQuota = c.opts.ReceiveMax
		c.opts.KeepAlive = authOpts.KeepAlive
		maxQoS := byte(0)
		if authOpts.MaximumQoS >= 2 {
			maxQoS = byte(1)
		}
		if connack.Properties == nil {
			connack.Properties = new(packets.Properties)
		}
		connack.Properties.SessionExpiryInterval = &authOpts.SessionExpiry
		connack.Properties.ReceiveMaximum = &authOpts.ReceiveMax
		connack.Properties.MaximumQoS = &maxQoS
		connack.Properties.RetainAvailable = common.Bool2Byte(authOpts.RetainAvailable)
		connack.Properties.TopicAliasMaximum = &c.opts.ServerTopicAliasMax
		connack.Properties.WildcardSubAvailable = common.Bool2Byte(authOpts.WildcardSubAvailable)
		connack.Properties.SubIDAvailable = common.Bool2Byte(authOpts.SubIDAvailable)
		connack.Properties.SharedSubAvailable = common.Bool2Byte(authOpts.SharedSubAvailable)
		connack.Properties.MaximumPacketSize = &authOpts.MaxPacketSize
		connack.Properties.ServerKeepAlive = &authOpts.KeepAlive
		connack.Properties.AssignedClientID = authOpts.AssignedClientID
		connack.Properties.ResponseInfo = authOpts.ResponseInfo
	}
	// KeepAlive
	if c.opts.KeepAlive != 0 {
		c.keepAlive = time.Duration(c.opts.KeepAlive/2+c.opts.KeepAlive) * time.Second
		_ = c.rwc.SetReadDeadline(time.Now().Add(c.keepAlive))
	}
	// new client session
	c.session = &models.Session{
		ClientID:    c.opts.ClientID,
		Will:        models.WillMsgFromConnect(conn),
		ConnectedAt: time.Now(),
	}
	// use default expiry if the client version is version3.1.1
	if packets.IsVersion3X(c.version) && !c.cleanStart {
		c.session.ExpiryInterval = uint32(time.Duration(c.srv.cfg.MQTT.SessionExpiry).Seconds())
	} else if conn.Properties != nil {
		c.session.WillDelayInterval = common.ConvertPtr(conn.WillProperties.WillDelayInterval, 0)
		c.session.ExpiryInterval = c.opts.SessionExpiry
	}
	return connack
}

func (c *client) authHandler(
	ctx context.Context, conn *packets.Connect, authOpts *models.AuthOptions,
) (*models.AuthResponse, *errors.Error) {
	authResponse, err := c.srv.hooks.OnEnhancedAuth(conn, authOpts)
	if err != nil || authResponse == nil {
		return nil, errors.NewError(consts.BadAuthMethod)
	}
	for authResponse.Continue {
		c.write(&packets.Auth{
			Code: consts.ContinueAuthentication,
			Properties: &packets.Properties{
				AuthMethod: conn.Properties.AuthMethod,
				AuthData:   authResponse.AuthData,
			},
		})
		select {
		case <-ctx.Done():
			return nil, errors.NewError(consts.MaxConnectTime)
		case p, ok := <-c.in:
			if !ok || p == nil {
				return nil, errors.ErrMalformed
			}
			auth, ok := p.(*packets.Auth)
			if !ok {
				return nil, errors.ErrMalformed
			}
			authResponse, err = c.srv.hooks.OnAuth(c, &models.AuthRequest{Auth: auth, Options: authOpts})
			if err != nil || authResponse == nil {
				return nil, errors.NewError(consts.BadAuthMethod)
			}
		}
	}
	return authResponse, nil
}

func (c *client) subscribeHandler(subPkg *packets.Subscribe) *errors.Error {
	suback := &packets.Suback{
		Version:    subPkg.Version,
		PacketID:   subPkg.PacketID,
		Properties: &packets.Properties{},
		Payload:    make([]consts.Code, len(subPkg.Topics)),
	}
	var subID uint32
	now := time.Now()
	if packets.IsVersion5(c.version) {
		if c.opts.SubIDAvailable && len(subPkg.Properties.SubscriptionIdentifier) != 0 {
			subID = subPkg.Properties.SubscriptionIdentifier[0]
		}
		if !c.srv.cfg.MQTT.SubscriptionIDAvailable && subID != 0 {
			return &errors.Error{
				Code: consts.SubIDNotSupported,
			}
		}
	}
	subReq := &models.SubscribeRequest{
		Subscribe: subPkg,
		Subscriptions: make(map[string]*struct {
			Sub   *models.Subscription
			Error error
		}),
		ID: subID,
	}

	for _, v := range subPkg.Topics {
		subReq.Subscriptions[v.Name] = &struct {
			Sub   *models.Subscription
			Error error
		}{Sub: new(models.Subscription).FromTopic(v, subID), Error: nil}
	}
	if ce := errors.Unwrap(c.srv.hooks.OnSubscribe(c, subReq)); ce != nil {
		if packets.IsVersion5(c.version) && c.opts.RequestProblemInfo {
			suback.Properties = c.properties(ce)
		}
		for k := range suback.Payload {
			if packets.IsVersion3X(c.version) {
				suback.Payload[k] = packets.SubscribeFailure
			} else {
				suback.Payload[k] = ce.Code
			}
		}
		c.write(suback)
		return nil
	}
	for k, v := range subPkg.Topics {
		sub := subReq.Subscriptions[v.Name].Sub
		subErr := errors.Unwrap(subReq.Subscriptions[v.Name].Error)
		var isShared bool
		code := sub.QoS
		if packets.IsVersion5(c.version) {
			if sub.ShareName != "" {
				isShared = true
				if !c.opts.SharedSubAvailable {
					code = consts.SharedSubNotSupported
				}
			}
			if !c.opts.SubIDAvailable && subID != 0 {
				code = consts.SubIDNotSupported
			}
			if !c.opts.WildcardSubAvailable {
				for _, f := range sub.TopicFilter {
					if f == '+' || f == '#' {
						code = consts.WildcardSubNotSupported
						break
					}
				}
			}
		}

		var subRs []models.SubscribeResult
		var err error
		if subErr != nil {
			code = subErr.Code
			if packets.IsVersion3X(c.version) {
				code = packets.SubscribeFailure
			}
		}
		if code < packets.SubscribeFailure {
			subRs, err = c.srv.persistence.Subscription().Subscribe(c.opts.ClientID, sub)
			if err != nil {
				c.logger.Error("failed to subscribe topic",
					"topic", v.Name,
					"qos", v.Qos,
					"remote_addr", c.rwc.RemoteAddr().String(),
					"err", err)
				code = packets.SubscribeFailure
			}
		}
		suback.Payload[k] = code
		if code < packets.SubscribeFailure {
			c.srv.hooks.OnSubscribed(c, sub)
			c.logger.Info("subscribe succeeded",
				"topic", sub.TopicFilter,
				"qos", sub.QoS,
				"retain_handling", sub.RetainHandling,
				"retain_as_published", sub.RetainAsPublished,
				"no_local", sub.NoLocal,
				"id", sub.ID,
				"remote_addr", c.rwc.RemoteAddr().String())
			// The spec does not specify whether the retain message should follow the 'no-local' option rule.
			// broker follows the mosquitto implementation which will send retain messages to no-local subscriptions.
			// For details: https://github.com/eclipse/mosquitto/issues/1796
			if !isShared && ((!subRs[0].AlreadyExisted && v.RetainHandling != 2) || v.RetainHandling == 0) {
				msgs := c.srv.persistence.Retained().GetMatchedMessages(sub.TopicFilter)
				for _, msg := range msgs {
					if msg.QoS > subRs[0].Subscription.QoS {
						msg.QoS = subRs[0].Subscription.QoS
					}
					msg.Dup = false
					if !sub.RetainAsPublished {
						msg.Retained = false
					}
					var expiry time.Time
					if msg.MessageExpiry != 0 {
						expiry = now.Add(time.Second * time.Duration(msg.MessageExpiry))
					}
					err = c.queueStore.Add(&models.QueueElem{
						At:     now,
						Expiry: expiry,
						MessageWithID: &models.Publish{
							Message: msg,
						},
					})
					if err != nil {
						var codesErr *errors.Error
						if errors.As(err, &codesErr) {
							return codesErr
						}
						return errors.NewError(consts.UnspecifiedError)
					}
				}
			}
		}
	}
	c.write(suback)
	return nil
}

func (c *client) publishHandler(pub *packets.Publish) *errors.Error {
	srv := c.srv
	var dup bool

	// check retain available
	if !c.opts.RetainAvailable && pub.Retain {
		return &errors.Error{
			Code: consts.RetainNotSupported,
		}
	}
	msg := models.MessageFromPublish(pub)
	// client topic alias -> topic
	if packets.IsVersion5(c.version) && pub.Properties.TopicAlias != nil {
		var err *errors.Error
		msg.Topic, err = c.topicAlias.Client(string(pub.TopicName), *pub.Properties.TopicAlias)
		if err != nil {
			c.logger.Debug("topic alias invalid",
				"topic_alias", *pub.Properties.TopicAlias,
				"server_topic_alias_max", c.opts.ServerTopicAliasMax,
				"topic", pub.TopicName,
				"payload", string(pub.Payload))
			return err
		}
	}
	// 服务端不能将$字符开头的主题名匹配通配符（#或+）开头的主题过滤器 [MQTT-4.7.2-1]。
	// 服务端应该阻止客户端使用这种主题名与其它客户端交换消息。服务端实现可以将$开头的主题名用作其他目的。
	// 此处直接拦截这种非法格式的publish消息
	if len(msg.Topic) > 0 && msg.Topic[0] == '$' {
		c.write(pub.NewPuback(consts.TopicNameInvalid, nil))
		return nil
	}

	if pub.Qos == packets.Qos2 {
		exist, err := c.unackStore.Set(pub.PacketID)
		if err != nil {
			return errors.Unwrap(err)
		}
		if exist {
			dup = true
		}
	}

	if pub.Retain {
		if len(pub.Payload) == 0 {
			srv.persistence.Retained().Remove(string(pub.TopicName))
		} else {
			srv.persistence.Retained().AddOrReplace(msg.Copy())
		}
	}

	var err error
	var topicMatched bool
	if !dup {
		if err = srv.hooks.OnMsgArrived(c, msg); err == nil {
			topicMatched = srv.deliver(c.opts.ClientID, msg)
		}
	}

	var ack packets.Packet
	// ack properties
	var ppt *packets.Properties
	code := consts.Success
	if packets.IsVersion5(c.version) {
		if !topicMatched && err == nil {
			code = consts.NotMatchingSubscribers
		}
		if codeErr := errors.Unwrap(err); codeErr != nil {
			ppt = c.properties(codeErr)
			code = codeErr.Code
		}
	}
	if pub.Qos == packets.Qos1 {
		ack = pub.NewPuback(code, ppt)
	}
	if pub.Qos == packets.Qos2 {
		ack = pub.NewPubrec(code, ppt)
		if code >= consts.UnspecifiedError {
			err = c.unackStore.Remove(pub.PacketID)
			if err != nil {
				return errors.Unwrap(err)
			}
		}
	}
	if ack != nil {
		c.write(ack)
	}
	return nil

}

func (c *client) pubackHandler(puback *packets.Puback) *errors.Error {
	err := c.queueStore.Remove(puback.PacketID)
	if err != nil {
		return errors.Unwrap(err)
	}
	c.packetIDLimiter.Release(puback.PacketID)
	c.logger.Debug("unset inflight message with pub ack", "packet_id", puback.PacketID)
	return nil
}

func (c *client) pubrelHandler(pubrel *packets.Pubrel) *errors.Error {
	err := c.unackStore.Remove(pubrel.PacketID)
	if err != nil {
		return errors.Unwrap(err)
	}
	pubcomp := pubrel.NewPubcomp()
	c.write(pubcomp)
	return nil
}

func (c *client) pubrecHandler(pubrec *packets.Pubrec) {
	if packets.IsVersion5(c.version) && pubrec.Code >= consts.UnspecifiedError {
		err := c.queueStore.Remove(pubrec.PacketID)
		c.packetIDLimiter.Release(pubrec.PacketID)
		if err != nil {
			c.Error(err)
		}
		return
	}
	pubrel := pubrec.NewPubrel()
	_, err := c.queueStore.Replace(&models.QueueElem{
		At: time.Now(),
		MessageWithID: &models.Pubrel{
			PacketID: pubrel.PacketID,
		}})
	if err != nil {
		c.Error(err)
	}
	c.write(pubrel)
}
func (c *client) pubcompHandler(pubcomp *packets.Pubcomp) {
	err := c.queueStore.Remove(pubcomp.PacketID)
	c.packetIDLimiter.Release(pubcomp.PacketID)
	if err != nil {
		c.Error(err)
	}
}
func (c *client) pingreqHandler(pingreq *packets.Pingreq) {
	resp := pingreq.NewPingresp()
	c.write(resp)
}
func (c *client) unsubscribeHandler(unSub *packets.Unsubscribe) {
	srv := c.srv
	unSuback := &packets.Unsuback{
		Version:    unSub.Version,
		PacketID:   unSub.PacketID,
		Properties: &packets.Properties{},
	}
	cs := make([]consts.Code, len(unSub.Topics))
	defer func() {
		if packets.IsVersion5(c.version) {
			unSuback.Payload = cs
		}
		c.write(unSuback)
	}()
	req := &models.UnsubscribeRequest{
		Unsubscribe: unSub,
		Unsubs: make(map[string]*struct {
			TopicName string
			Error     error
		}),
	}
	for _, v := range unSub.Topics {
		req.Unsubs[v] = &struct {
			TopicName string
			Error     error
		}{TopicName: v}
	}
	err := srv.hooks.OnUnsubscribe(c, req)
	if ce := errors.Unwrap(err); ce != nil {
		unSuback.Properties = c.properties(ce)
		for k := range cs {
			cs[k] = ce.Code
		}
		return
	}
	for k, v := range unSub.Topics {
		code := consts.Success
		topicName := req.Unsubs[v].TopicName
		ce := errors.Unwrap(req.Unsubs[v].Error)
		if ce != nil {
			code = ce.Code
		}
		if code == consts.Success {
			err := srv.persistence.Subscription().Unsubscribe(c.opts.ClientID, topicName)
			if ce := errors.Unwrap(err); ce != nil {
				code = ce.Code
			}
		}
		l := c.logger.With(
			"topic", topicName,
			"remote_addr", c.rwc.RemoteAddr().String(),
		)
		if code == consts.Success {
			srv.hooks.OnUnsubscribed(c, topicName)
			l.Info("unsubscribed succeed")
		} else {
			l.Warn("unsubscribed failed")
		}
		cs[k] = code
	}
}

func (c *client) reAuthHandler(auth *packets.Auth) *errors.Error {
	srv := c.srv
	// default code
	code := consts.Success
	resp, err := srv.hooks.OnReAuth(c, auth)
	if err != nil {
		return errors.Unwrap(err)
	}
	if resp.Continue {
		code = consts.ContinueAuthentication
	}
	c.write(&packets.Auth{
		Code: code,
		Properties: &packets.Properties{
			AuthMethod: c.opts.AuthMethod,
			AuthData:   resp.AuthData,
		},
	})
	return nil
}

func (c *client) disconnectHandler(dis *packets.Disconnect) *errors.Error {
	if packets.IsVersion5(c.version) {
		if dis.Properties != nil && dis.Properties.SessionExpiryInterval != nil {
			c.session.ExpiryInterval = *dis.Properties.SessionExpiryInterval
		}
	}
	c.session.Will = nil
	return nil
}
