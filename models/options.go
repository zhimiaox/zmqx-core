package models

import (
	"github.com/zhimiaox/zmqx-core/packets"
)

// ClientOptions is the options which controls how the server interacts with the client.
// It will be set after the client has connected.
type ClientOptions struct {
	// ClientID is the client id for the client.
	ClientID string
	// Username is the username for the client.
	Username string
	// KeepAlive is the keep alive time in seconds for the client.
	// The server will close the client if no there is no packet has been received for 1.5 times the KeepAlive time.
	KeepAlive uint16
	// SessionExpiry is the session expiry interval in seconds.
	// If the client version is v5, this value will be set into CONNACK Session Expiry Interval property.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082
	SessionExpiry uint32
	// MaxInflight limits the number of QoS 1 and QoS 2 publications that the client is willing to process concurrently.
	// For v3 client, it is default to config.MQTT.MaxInflight.
	// For v5 client, it is the minimum of config.MQTT.MaxInflight and Receive Maximum property in CONNECT packet.
	MaxInflight uint16
	// ReceiveMax limits the number of QoS 1 and QoS 2 publications that the server is willing to process concurrently for the Client.
	// If the client version is v5, this value will be set into Receive Maximum property in CONNACK packet.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083
	ReceiveMax uint16
	// ClientMaxPacketSize is the maximum packet size that the client is willing to accept.
	// The server will drop the packet if it exceeds ClientMaxPacketSize.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050
	ClientMaxPacketSize uint32
	// ServerMaxPacketSize is the maximum packet size that the server is willing to accept from the client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086
	ServerMaxPacketSize uint32
	// ClientTopicAliasMax is the highest value that the client will accept as a Topic Alias sent by the server.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051
	ClientTopicAliasMax uint16
	// ServerTopicAliasMax is the highest value that the server will accept as a Topic Alias sent by the client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088
	ServerTopicAliasMax uint16
	// RequestProblemInfo is the value to indicate whether the Reason String or User properties should be sent in the case of failures.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053
	RequestProblemInfo bool
	// UserProperties is the user properties provided by the client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090
	UserProperties []*packets.UserProperty
	// WildcardSubAvailable indicates whether the client is permitted to send retained messages.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091
	RetainAvailable bool
	// WildcardSubAvailable indicates whether the client is permitted to subscribe Wildcard Subscriptions.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091
	WildcardSubAvailable bool
	// SubIDAvailable indicates whether the client is permitted to set Subscription Identifiers.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092
	SubIDAvailable bool
	// SharedSubAvailable indicates whether the client is permitted to subscribe Shared Subscriptions.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093
	SharedSubAvailable bool
	// AuthMethod is the auth method send by the client.
	// Only MQTT v5 client can set this value.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901055
	AuthMethod []byte
}

// AuthOptions provides several options which controls how the server interacts with the client.
// The default value of these options is defined in the configuration file.
type AuthOptions struct {
	// SessionExpiry is session expired time in seconds.
	SessionExpiry uint32
	// ReceiveMax limits the number of QoS 1 and QoS 2 publications that the server is willing to process concurrently for the client.
	// If the client version is v5, this value will be set into  Receive Maximum property in CONNACK packet.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083
	ReceiveMax uint16
	// MaximumQoS is the highest QOS level permitted for a Publish.
	MaximumQoS uint8
	// MaxPacketSize is the maximum packet size that the server is willing to accept from the client.
	// If the client version is v5, this value will be set into Receive Maximum property in CONNACK packet.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086
	MaxPacketSize uint32
	// TopicAliasMax indicates the highest value that the server will accept as a Topic Alias sent by the client.
	// The server uses this value to limit the number of Topic Aliases that it is willing to hold on this connection.
	// This option only affect v5 client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088
	TopicAliasMax uint16
	// RetainAvailable indicates whether the server supports retained messages.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085
	RetainAvailable bool
	// WildcardSubAvailable indicates whether the server supports Wildcard Subscriptions.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091
	WildcardSubAvailable bool
	// SubIDAvailable indicates whether the server supports Subscription Identifiers.
	// This option only affect v5 client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092
	SubIDAvailable bool
	// SharedSubAvailable indicates whether the server supports Shared Subscriptions.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093
	SharedSubAvailable bool
	// KeepAlive is the keep alive time assigned by the server.
	// This option only affect v5 client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094
	KeepAlive uint16
	// UserProperties is be used to provide additional information to the client.
	// This option only affect v5 client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090
	UserProperties []*packets.UserProperty
	// AssignedClientID allows the server to assign a client id for the client.
	// It will override the client id in the connectPkg packet.
	AssignedClientID []byte
	// ResponseInfo is used as the basis for creating a Response Topic.
	// This option only affect v5 client.
	// See: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095
	ResponseInfo []byte
	// MaxInflight limits the number of QoS 1 and QoS 2 publications that the client is willing to process concurrently.
	MaxInflight uint16
}
