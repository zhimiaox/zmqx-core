package persistence

import (
	"github.com/zhimiaox/zmqx-core/models"
)

// Subscription is the interface used by models.server to handler the operations of subscriptions.
// This interface provides the ability for extensions to interact with the subscriptions.
// Notice:
// This methods will not trigger any models hooks.
type Subscription interface {
	// Subscribe adds subscriptions to a specific client.
	// Notice:
	// This method will succeed even if the client is not exists, the subscriptions
	// will affect the new client with the client id.
	Subscribe(clientID string, subscriptions ...*models.Subscription) ([]models.SubscribeResult, error)
	// Unsubscribe removes subscriptions of a specific client.
	Unsubscribe(clientID string, topics ...string) error
	// UnsubscribeAll removes all subscriptions of a specific client.
	UnsubscribeAll(clientID string) error
	// Iterate iterates all subscriptions. The callback is called once for each subscription.
	// If callback return false, the iteration will be stopped.
	// Notice:
	// The results are not sorted in any way, no ordering of any kind is guaranteed.
	// This method will walk through all subscriptions,
	// so it is a very expensive operation. Do not call it frequently.
	Iterate(fn models.SubscriptionIterateFn, options models.IterationOptions)
}
