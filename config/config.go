package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/packets"

	"github.com/BurntSushi/toml"
)

func New() *Config {
	return &Config{
		Server: Server{
			Debug:       false,
			NodeID:      common.NanoID(),
			TCP:         &TCPListen{Listen: ":1883"},
			Websocket:   nil,
			Persistence: Persistence{Type: "memory"},
		},
		MQTT: MQTT{
			SessionExpiry:              consts.Duration(2 * time.Hour),
			SessionExpiryCheckInterval: consts.Duration(20 * time.Second),
			MessageExpiry:              consts.Duration(2 * time.Hour),
			InflightExpiry:             consts.Duration(30 * time.Second),
			MaxPacketSize:              268435456,
			ReceiveMax:                 100,
			MaxKeepAlive:               300,
			TopicAliasMax:              10,
			SubscriptionIDAvailable:    true,
			SharedSubAvailable:         true,
			WildcardAvailable:          true,
			RetainAvailable:            true,
			MaxQueuedMsg:               1000,
			MaxInflight:                100,
			MaximumQoS:                 2,
			QueueQos0Msg:               true,
			DeliveryMode:               "onlyonce",
			AllowZeroLenClientID:       true,
		},
	}
}

type Config struct {
	Server Server `toml:"server"`
	MQTT   MQTT   `toml:"mqtt"`
}

type Server struct {
	Debug       bool             `toml:"debug" default:"false"`
	NodeID      string           `toml:"node_id"`
	TCP         *TCPListen       `toml:"tcp"`
	Websocket   *WebsocketListen `toml:"websocket"`
	Persistence Persistence      `toml:"persistence"`
}

type TCPListen struct {
	Listen string `toml:"listen" default:":1883"`
	TLS    *TLS   `toml:"tls"`
}

type WebsocketListen struct {
	Listen string `toml:"listen" default:":8883"`
	Path   string `toml:"path" default:"/"`
	TLS    *TLS   `toml:"tls"`
}

type TLS struct {
	Cert string `toml:"cert"`
	Key  string `toml:"key"`
}

type Persistence struct {
	Type  string `toml:"type"`
	Redis *struct {
		Addr     []string `toml:"addr"`
		Password string   `toml:"password"`
		Database int      `toml:"database"`
	}
}

type MQTT struct {
	// SessionExpiry is the maximum session expiry interval in seconds.
	SessionExpiry consts.Duration `toml:"session_expiry" default:"2h"`
	// SessionExpiryCheckInterval is the interval time for a session expiry checker to check whether there
	// are expired sessions.
	SessionExpiryCheckInterval consts.Duration `toml:"session_expiry_check_interval" default:"20s"`
	// MessageExpiry is the maximum lifetime of the message in seconds.
	// If a message in the queue is not sent in MessageExpiry time, it will be removed, which means it will not be sent to the subscriber.
	MessageExpiry consts.Duration `toml:"message_expiry" default:"2h"`
	// InflightExpiry is the lifetime of the "inflight" message in seconds.
	// If an "inflight" message is not acknowledged by a client in InflightExpiry time, it will be removed when the message queue is full.
	InflightExpiry consts.Duration `toml:"inflight_expiry" default:"30s"`
	// MaxPacketSize is the maximum packet size that the server is willing to accept from the client
	MaxPacketSize uint32 `toml:"max_packet_size" default:"268435456"`
	// ReceiveMax limits the number of QoS 1 and QoS 2 publications that the server is willing to process concurrently for the client.
	ReceiveMax uint16 `toml:"server_receive_maximum" default:"100"`
	// MaxKeepAlive is the maximum keep live time in seconds allows by the server.
	// If the client requests a keepalive time bigger than MaxKeepalive,
	// the server will use MaxKeepAlive as the keepalive time.
	// In this case, if the client version is v5, the server will set MaxKeepalive into CONNACK to inform the client.
	// But if the client version is 3.x, the server has no way to inform the client that the keepalive time has been changed.
	MaxKeepAlive uint16 `toml:"max_keepalive" default:"300"`
	// TopicAliasMax indicates the highest value that the server will accept as a Topic Alias sent by the client.
	// No-op if the client version is MQTTv3.x
	TopicAliasMax uint16 `toml:"topic_alias_maximum" default:"10"`
	// SubscriptionIDAvailable indicates whether the server supports Subscription Identifiers.
	// No-op if the client version is MQTTv3.x.
	SubscriptionIDAvailable bool `toml:"subscription_identifier_available" default:"true"`
	// SharedSubAvailable indicates whether the server supports Shared Subscriptions.
	SharedSubAvailable bool `toml:"shared_subscription_available" default:"true"`
	// WildcardSubAvailable indicates whether the server supports Wildcard Subscriptions.
	WildcardAvailable bool `toml:"wildcard_subscription_available" default:"true"`
	// RetainAvailable indicates whether the server supports retained messages.
	RetainAvailable bool `toml:"retain_available" default:"true"`
	// MaxQueuedMsg is the maximum queue length of the outgoing messages.
	// If the queue is full, some message will be dropped.
	// The message dropping strategy is described in the document of the persistence/queue.Store interface.
	MaxQueuedMsg int `toml:"max_queued_messages" default:"1000"`
	// MaxInflight limits inflight message length of the outgoing messages.
	// Inflight message is also stored in the message queue, so it must be less than or equal to MaxQueuedMsg.
	// The Inflight message is the QoS 1 or QoS 2 message that has been sent out to a client but not been acknowledged yet.
	MaxInflight uint16 `toml:"max_inflight" default:"100"`
	// MaximumQoS is the highest QOS level permitted for a Publish.
	MaximumQoS uint8 `toml:"maximum_qos" default:"2"`
	// QueueQos0Msg indicates whether to store QoS 0 message for a offline session.
	QueueQos0Msg bool `toml:"queue_qos0_messages" default:"true"`
	// DeliveryMode is the delivery mode. The possible value can be "overlap" or "onlyonce".
	// It is possible for a client’s subscriptions to overlap so that a published message might match multiple filters.
	// When set to "overlap", the server will deliver one message for each matching subscription and respecting the subscription’s QoS in each case.
	// When set to "onlyonce",the server will deliver the message to the client respecting the maximum QoS of all the matching subscriptions.
	DeliveryMode string `toml:"delivery_mode" default:"onlyonce"`
	// AllowZeroLenClientID indicates whether to allow a client to connect with empty client id.
	AllowZeroLenClientID bool `toml:"allow_zero_length_client_id" default:"true"`
}

func ParseConfigFile(file string) (*Config, error) {
	config := New()
	if _, err := toml.DecodeFile(file, &config); err != nil {
		return nil, err
	}
	if err := Validate(config); err != nil {
		return nil, err
	}
	return config, nil
}

func Validate(cfg *Config) error {
	if cfg.MQTT.MaximumQoS > packets.Qos2 {
		return fmt.Errorf("invalid maximum_qos: %d", cfg.MQTT.MaximumQoS)
	}
	if cfg.MQTT.MaxQueuedMsg <= 0 {
		return fmt.Errorf("invalid max_queued_messages : %d", cfg.MQTT.MaxQueuedMsg)
	}
	if cfg.MQTT.ReceiveMax == 0 {
		return fmt.Errorf("server_receive_maximum cannot be 0")
	}
	if cfg.MQTT.MaxPacketSize == 0 {
		return fmt.Errorf("max_packet_size cannot be 0")
	}
	if cfg.MQTT.MaxPacketSize > packets.MaxSize {
		return fmt.Errorf("max_packet_size cannot be out max size")
	}
	if cfg.MQTT.MaxInflight == 0 {
		return fmt.Errorf("max_inflight cannot be 0")
	}
	if cfg.MQTT.DeliveryMode != consts.Overlap && cfg.MQTT.DeliveryMode != consts.OnlyOnce {
		return fmt.Errorf("invalid delivery_mode: %s", cfg.MQTT.DeliveryMode)
	}
	if cfg.MQTT.MaxQueuedMsg < int(cfg.MQTT.MaxInflight) {
		return fmt.Errorf("max_queued_message cannot be less than max_inflight")
	}
	if cfg.Server.NodeID == "" {
		return fmt.Errorf("invalid server NodeID with empty")
	}
	if strings.Contains(cfg.Server.NodeID, ":") {
		return fmt.Errorf("invalid server NodeID with : ")
	}
	return nil
}
