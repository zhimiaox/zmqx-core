package persistence

type Persistence interface {
	TryLock(clientID string) bool
	Unlock(clientID string)
	Subscription() Subscription
	Session() Session
	Retained() Retained
	Queue(clientID string) Queue
	Unack(clientID string) Unack
	Will() Will
	DestroyClient(clientID string)
}
