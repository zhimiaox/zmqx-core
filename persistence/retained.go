package persistence

import "github.com/zhimiaox/zmqx-core/models"

// RetainedIterateFn is the callback function used by iterate()
// Return false means to stop the iteration.
type RetainedIterateFn func(message *models.Message) bool

// Retained is the interface used by models.server and external logic to handler the operations of retained messages.
// User can get the implementation from models.Server interface.
// This interface provides the ability for extensions to interact with the retained message store.
// Notice:
// This methods will not trigger any models hooks.
type Retained interface {
	// AddOrReplace adds or replaces a retained message.
	AddOrReplace(message *models.Message)
	// Remove removes a retained message.
	Remove(topicName string)
	// GetMatchedMessages returns the retained messages that match the passed topic filter.
	GetMatchedMessages(topicFilter string) []*models.Message
}
