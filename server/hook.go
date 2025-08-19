package server

import (
	"net"

	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
)

type OnStop func()
type OnAccept func(conn net.Conn) error
type OnBasicAuth func(conn *packets.Connect, authOpts *models.AuthOptions) error
type OnEnhancedAuth func(conn *packets.Connect, authOpts *models.AuthOptions) (*models.AuthResponse, error)
type OnAuth func(client Client, req *models.AuthRequest) (*models.AuthResponse, error)
type OnReAuth func(client Client, auth *packets.Auth) (*models.AuthResponse, error)
type OnSubscribe func(client Client, req *models.SubscribeRequest) error
type OnSubscribed func(client Client, req *models.Subscription)
type OnUnsubscribe func(client Client, req *models.UnsubscribeRequest) error
type OnUnsubscribed func(client Client, topicName string)
type OnMsgArrived func(client Client, message *models.Message) error
type OnConnected func(client Client)
type OnClosed func(clientID string)
type OnSessionCreated func(client Client)
type OnSessionResumed func(client Client)
type OnSessionTerminated func(clientID string)
type OnDelivered func(client Client, msg *packets.Publish)
type OnMsgDropped func(clientID string, msg *models.QueueElem, err error)
type OnWillPublish func(will *models.WillMsg)
type OnWillPublished func(will *models.WillMsg)

type Hooks interface {
	OnStop()
	OnAccept(conn net.Conn) error
	OnBasicAuth(conn *packets.Connect, authOpts *models.AuthOptions) error
	OnEnhancedAuth(conn *packets.Connect, authOpts *models.AuthOptions) (*models.AuthResponse, error)
	OnAuth(client Client, req *models.AuthRequest) (*models.AuthResponse, error)
	OnReAuth(client Client, auth *packets.Auth) (*models.AuthResponse, error)
	OnSubscribe(client Client, req *models.SubscribeRequest) error
	OnSubscribed(client Client, req *models.Subscription)
	OnUnsubscribe(client Client, req *models.UnsubscribeRequest) error
	OnUnsubscribed(client Client, topicName string)
	OnMsgArrived(client Client, message *models.Message) error
	OnConnected(client Client)
	OnClosed(clientID string)
	OnSessionCreated(client Client)
	OnSessionResumed(client Client)
	OnSessionTerminated(clientID string)
	OnDelivered(client Client, msg *packets.Publish)
	OnMsgDropped(clientID string, msg *models.QueueElem, err error)
	OnWillPublish(will *models.WillMsg)
	OnWillPublished(will *models.WillMsg)
}

type hooks struct {
	onStop              OnStop
	onAccept            OnAccept
	onBasicAuth         OnBasicAuth
	onEnhancedAuth      OnEnhancedAuth
	onAuth              OnAuth
	onReAuth            OnReAuth
	onSubscribe         OnSubscribe
	onSubscribed        OnSubscribed
	onUnsubscribe       OnUnsubscribe
	onUnsubscribed      OnUnsubscribed
	onMsgArrived        OnMsgArrived
	onConnected         OnConnected
	onClosed            OnClosed
	onSessionCreated    OnSessionCreated
	onSessionResumed    OnSessionResumed
	onSessionTerminated OnSessionTerminated
	onDelivered         OnDelivered
	onMsgDropped        OnMsgDropped
	onWillPublish       OnWillPublish
	onWillPublished     OnWillPublished
}

func WithOnStop(onStop OnStop) Hook {
	return func(impl *hooks) {
		impl.onStop = onStop
	}
}

func (h *hooks) OnStop() {
	if h.onStop != nil {
		h.onStop()
	}
}

func (h *hooks) OnWillPublish(will *models.WillMsg) {
	if h.onWillPublish != nil {
		h.onWillPublish(will)
	}
}

func (h *hooks) OnWillPublished(will *models.WillMsg) {
	if h.onWillPublished != nil {
		h.onWillPublished(will)
	}
}

func WithOnMsgDropped(onMsgDropped OnMsgDropped) Hook {
	return func(impl *hooks) {
		impl.onMsgDropped = onMsgDropped
	}
}
func (h *hooks) OnMsgDropped(clientID string, msg *models.QueueElem, err error) {
	if h.onMsgDropped != nil {
		h.onMsgDropped(clientID, msg, err)
	}
}

func WithOnDelivered(onDelivered OnDelivered) Hook {
	return func(impl *hooks) {
		impl.onDelivered = onDelivered
	}
}

func (h *hooks) OnDelivered(client Client, msg *packets.Publish) {
	if h.onDelivered != nil {
		h.onDelivered(client, msg)
	}
}

func WithOnAccept(onAccept OnAccept) Hook {
	return func(impl *hooks) {
		impl.onAccept = onAccept
	}
}
func (h *hooks) OnAccept(conn net.Conn) error {
	if h.onAccept != nil {
		return h.onAccept(conn)
	}
	return nil
}

func WithOnBasicAuth(onBasicAuth OnBasicAuth) Hook {
	return func(impl *hooks) {
		impl.onBasicAuth = onBasicAuth
	}
}
func (h *hooks) OnBasicAuth(conn *packets.Connect, authOpts *models.AuthOptions) error {
	if h.onBasicAuth != nil {
		return h.onBasicAuth(conn, authOpts)
	}
	return nil
}

func WithOnEnhancedAuth(onEnhancedAuth OnEnhancedAuth) Hook {
	return func(impl *hooks) {
		impl.onEnhancedAuth = onEnhancedAuth
	}
}
func (h *hooks) OnEnhancedAuth(conn *packets.Connect, authOpts *models.AuthOptions) (*models.AuthResponse, error) {
	if h.onEnhancedAuth != nil {
		return h.onEnhancedAuth(conn, authOpts)
	}
	return nil, nil
}

func WithOnAuth(onAuth OnAuth) Hook {
	return func(impl *hooks) {
		impl.onAuth = onAuth
	}
}
func (h *hooks) OnAuth(client Client, req *models.AuthRequest) (*models.AuthResponse, error) {
	if h.onAuth != nil {
		return h.onAuth(client, req)
	}
	return nil, nil
}

func WithOnReAuth(onReAuth OnReAuth) Hook {
	return func(impl *hooks) {
		impl.onReAuth = onReAuth
	}
}
func (h *hooks) OnReAuth(client Client, auth *packets.Auth) (*models.AuthResponse, error) {
	if h.onReAuth != nil {
		return h.onReAuth(client, auth)
	}
	return nil, nil
}

func WithOnSubscribe(onSubscribe OnSubscribe) Hook {
	return func(impl *hooks) {
		impl.onSubscribe = onSubscribe
	}
}
func (h *hooks) OnSubscribe(client Client, req *models.SubscribeRequest) error {
	if h.onSubscribe != nil {
		return h.onSubscribe(client, req)
	}
	return nil
}

func WithOnSubscribed(onSubscribed OnSubscribed) Hook {
	return func(impl *hooks) {
		impl.onSubscribed = onSubscribed
	}
}
func (h *hooks) OnSubscribed(client Client, req *models.Subscription) {
	if h.onSubscribed != nil {
		h.onSubscribed(client, req)
	}
}

func WithOnUnsubscribe(onUnsubscribe OnUnsubscribe) Hook {
	return func(impl *hooks) {
		impl.onUnsubscribe = onUnsubscribe
	}
}
func (h *hooks) OnUnsubscribe(client Client, req *models.UnsubscribeRequest) error {
	if h.onUnsubscribe != nil {
		return h.onUnsubscribe(client, req)
	}
	return nil
}

func WithOnUnsubscribed(onUnsubscribed OnUnsubscribed) Hook {
	return func(impl *hooks) {
		impl.onUnsubscribed = onUnsubscribed
	}
}
func (h *hooks) OnUnsubscribed(client Client, req string) {
	if h.onUnsubscribed != nil {
		h.onUnsubscribed(client, req)
	}
}

func WithOnMsgArrived(onMsgArrived OnMsgArrived) Hook {
	return func(impl *hooks) {
		impl.onMsgArrived = onMsgArrived
	}
}
func (h *hooks) OnMsgArrived(client Client, msg *models.Message) error {
	if h.onMsgArrived != nil {
		return h.onMsgArrived(client, msg)
	}
	return nil
}

func WithOnConnected(onConnected OnConnected) Hook {
	return func(impl *hooks) {
		impl.onConnected = onConnected
	}
}
func (h *hooks) OnConnected(client Client) {
	if h.onConnected != nil {
		h.onConnected(client)
	}
}

func WithOnClosed(onClosed OnClosed) Hook {
	return func(impl *hooks) {
		impl.onClosed = onClosed
	}
}
func (h *hooks) OnClosed(clientID string) {
	if h.onClosed != nil {
		h.onClosed(clientID)
	}
}

func WithOnSessionCreated(onSessionCreated OnSessionCreated) Hook {
	return func(impl *hooks) {
		impl.onSessionCreated = onSessionCreated
	}
}
func (h *hooks) OnSessionCreated(client Client) {
	if h.onSessionCreated != nil {
		h.onSessionCreated(client)
	}
}

func WithOnSessionResumed(onSessionResumed OnSessionResumed) Hook {
	return func(impl *hooks) {
		impl.onSessionResumed = onSessionResumed
	}
}
func (h *hooks) OnSessionResumed(client Client) {
	if h.onSessionResumed != nil {
		h.onSessionResumed(client)
	}
}

func WithOnSessionTerminated(onSessionTerminated OnSessionTerminated) Hook {
	return func(impl *hooks) {
		impl.onSessionTerminated = onSessionTerminated
	}
}
func (h *hooks) OnSessionTerminated(clientID string) {
	if h.onSessionTerminated != nil {
		h.onSessionTerminated(clientID)
	}
}

type Hook func(impl *hooks)

func NewHooks(hook ...Hook) Hooks {
	h := &hooks{}
	for _, fn := range hook {
		fn(h)
	}
	return h
}
