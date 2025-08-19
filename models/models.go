package models

import (
	"net"

	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/packets"

	"github.com/gorilla/websocket"
)

type AuthRequest struct {
	Auth    *packets.Auth
	Options *AuthOptions
}

// AuthResponse 认证结果
type AuthResponse struct {
	Continue bool
	AuthData []byte
}

// SubscribeRequest represents the subscribe request made by a SUBSCRIBE packet.
type SubscribeRequest struct {
	// Subscribe is the SUBSCRIBE packet. It is immutable, do not edit.
	Subscribe *packets.Subscribe
	// Subscriptions wraps all subscriptions by the full topic name.
	// You can modify the value of the map to edit the subscription. But must not change the length of the map.
	Subscriptions map[string]*struct {
		// Sub is the subscription.
		Sub *Subscription
		// Error indicates whether to allow the subscription.
		// Return nil means it is allowed to make the subscription.
		// Return an error means it is not allow to make the subscription.
		// It is recommended to use *codes.Error if you want to disallow the subscription. e.g:&codes.Error{Code:codes.NotAuthorized}
		// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178
		Error error
	}
	// ID is the subscription id, this value will override the id of subscriptions in Subscriptions.Sub.
	// This field take no effect on v3 client.
	ID uint32
}

// GrantQoS grants the qos to the subscription for the given topic name.
func (s *SubscribeRequest) GrantQoS(topicName string, qos packets.QoS) *SubscribeRequest {
	if sub := s.Subscriptions[topicName]; sub != nil {
		sub.Sub.QoS = qos
	}
	return s
}

// Reject rejects the subscription for the given topic name.
func (s *SubscribeRequest) Reject(topicName string, err error) {
	if sub := s.Subscriptions[topicName]; sub != nil {
		sub.Error = err
	}
}

// SetID sets the subscription id for the subscriptions
func (s *SubscribeRequest) SetID(id uint32) *SubscribeRequest {
	s.ID = id
	return s
}

// UnsubscribeRequest is the input param for OnSubscribed hook.
type UnsubscribeRequest struct {
	// Unsubscribe is the UNSUBSCRIBE packet. It is immutable, do not edit.
	Unsubscribe *packets.Unsubscribe
	// Unsubs groups all unsubscribe topic by the full topic name.
	// You can modify the value of the map to edit the unsubscribe topic. But you cannot change the length of the map.
	Unsubs map[string]*struct {
		// TopicName is the topic that is going to unsubscribe.
		TopicName string
		// Error indicates whether to allow the unsubscription.
		// Return nil means it is allowed to unsubscribe the topic.
		// Return an error means it is not allow to unsubscribe the topic.
		// It is recommended to use *codes.Error if you want to disallow the unsubscription. e.g:&codes.Error{Code:codes.NotAuthorized}
		// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194
		Error error
	}
}

// Reject rejects the subscription for the given topic name.
func (u *UnsubscribeRequest) Reject(topicName string, err error) {
	if sub := u.Unsubs[topicName]; sub != nil {
		sub.Error = err
	}
}

// MsgArrivedRequest is the input param for OnMsgArrived hook.
type MsgArrivedRequest struct {
	// Publish is the origin MQTT PUBLISH packet, it is immutable. DO NOT EDIT.
	Publish *packets.Publish
	// Message is the message that is going to be passed to topic match process.
	// The caller can modify it.
	Message *Message
	// IterationOptions provides the ability to change the options of topic matching process.
	// In most of the cases, you don't need to modify it.
	// The default value is:
	// 	subscription.IterationOptions{
	//		Type:      subscription.TypeAll,
	//		MatchType: subscription.MatchFilter,
	//		TopicName: msg.Topic,
	//	}
	// The user of this field is the federation plugin.
	// It will change the Type from subscription.TypeAll to subscription.subscription.TypeAll ^ subscription.TypeShared
	// that will prevent publishing the shared message to local client.
	IterationOptions IterationOptions
}

// Drop drops the message, so the message will not be delivered to any clients.
func (m *MsgArrivedRequest) Drop() {
	m.Message = nil
}

// WsConn implements the io.readWriter
type WsConn struct {
	net.Conn
	W *websocket.Conn
}

func (ws *WsConn) Close() error {
	return ws.Conn.Close()
}

func (ws *WsConn) Read(p []byte) (n int, err error) {
	msgType, r, err := ws.W.NextReader()
	if err != nil {
		return 0, err
	}
	if msgType != websocket.BinaryMessage {
		return 0, errors.New("invalid websocket message type")
	}
	return r.Read(p)
}

func (ws *WsConn) Write(p []byte) (n int, err error) {
	err = ws.W.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), err
}
