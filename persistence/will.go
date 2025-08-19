package persistence

import "github.com/zhimiaox/zmqx-core/models"

type Will interface {
	Add(msg *models.WillMsg)
	Remove(clientID string)
	// Event 遗嘱消息下发(集群下消息应当只被单节点接收)
	Event(handler func(msg *models.WillMsg))
}
