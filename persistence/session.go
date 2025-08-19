package persistence

import "github.com/zhimiaox/zmqx-core/models"

type Session interface {
	Set(session *models.Session) error
	Get(clientID string) (*models.Session, error)
	Remove(clientID string) error
	Event(handler func(clientID string))
}
