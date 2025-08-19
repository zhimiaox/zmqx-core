package persistence

import (
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
)

// QueueInitOptions is used to pass some required client information to the queue.Init()
type QueueInitOptions struct {
	// CleanStart is the cleanStart field in the connect packet.
	CleanStart bool
	// Version is the client MQTT protocol version.
	Version packets.Version
	// ReadBytesLimit indicates the maximum publish size that is allow to read.
	ReadBytesLimit uint32
	// 消息丢弃通知
	OnMsgDropped func(clientID string, msg *models.QueueElem, err error)
}

// Queue represents a queue store for one client.
type Queue interface {
	// Close will be called when the client disconnect.
	// This method must unblock the Read method.
	Close()
	// Init will be called when the client connect.
	// If opts.CleanStart set to true, the implementation should remove any associated data in backend store.
	// If it sets to false, the implementation should be able to retrieve the associated data from backend store.
	// The opts.version indicates the protocol version of the connected client, it is mainly used to calculate the publish packet size.
	Init(opts *QueueInitOptions) error
	Clean() error
	// Add inserts a elem to the queue.
	// When the len of queue is reaching the maximum setting, the implementation should drop messages according the following priorities:
	// 1. Drop the expired inflight message.
	// 2. Drop the current elem if there is no more non-inflight messages.
	// 3. Drop expired non-inflight message.
	// 4. Drop qos0 message.
	// 5. Drop the front message.
	// See queue.mem for more details.
	Add(elem *models.QueueElem) error
	// Replace replaces the PUBLISH with the PUBREL with the same packet id.
	Replace(elem *models.QueueElem) (replaced bool, err error)

	// Read reads a batch of new message (non-inflight) from the store. The qos0 messages will be removed after read.
	// The size of the batch will be less than or equal to the size of the given packet id list.
	// The implementation must remove and do not return any :
	// 1. expired messages
	// 2. publish message which exceeds the QueueInitOptions.ReadBytesLimit
	// while reading.
	// The caller must call ReadInflight first to read all inflight message before calling this method.
	// Calling this method will be blocked until there are any new messages can be read or the store has been closed.
	// If the store has been closed, returns nil, ErrClosed.
	Read(packetIDs []packets.PacketID) ([]*models.QueueElem, error)

	// ReadInflight reads at most maxSize inflight messages.
	// The caller must call this method to read all inflight messages before calling Read method.
	// Returning 0 length elems means all inflight messages have been read.
	ReadInflight(maxSize uint16) (elems []*models.QueueElem, err error)

	// Remove removes the elem for a given id.
	Remove(pid packets.PacketID) error
}
