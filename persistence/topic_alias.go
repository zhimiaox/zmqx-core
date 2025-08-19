package persistence

import (
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
)

// TopicAliasManager manage the topic alias for a V5 client.
// 客户端和服务器分别把持不同的alias映射，因此要分开存储
type TopicAliasManager interface {
	// Server 获取服务端下发主题名别名
	// For examples:
	// If the Publish alias exist and the manager decides to use the alias, it return the alias number and true.
	// If the Publish alias exist, but the manager decides not to use alias, it return 0 and true.
	// If the Publish alias not exist and the manager decides to assign a new alias, it return the new alias and false.
	// If the Publish alias not exist, but the manager decides not to assign alias, it return the 0 and false.
	Server(pubTopicName string) (alias uint16, exist bool)
	// Client 获取客户端上送报文别名对应主题名
	Client(pubTopicName string, alias uint16) (topicName string, err *errors.Error)
}

type topicAlias struct {
	serverMaxAliasNum    uint16
	clientMaxAliasNum    uint16
	serverCurAliasOffset uint16
	server               map[string]uint16
	client               map[uint16]string
}

// NewTopicAliasManager is the constructor of TopicAlias.
func NewTopicAliasManager(clientMax, serverMax uint16) TopicAliasManager {
	return &topicAlias{
		serverMaxAliasNum:    serverMax,
		clientMaxAliasNum:    clientMax,
		serverCurAliasOffset: 0,
		server:               make(map[string]uint16),
		client:               make(map[uint16]string),
	}
}

func (ta *topicAlias) Client(pubTopicName string, alias uint16) (string, *errors.Error) {
	if alias >= ta.serverMaxAliasNum || alias == 0 {
		return "", errors.NewError(consts.TopicAliasInvalid)
	}
	if pubTopicName == "" {
		topicName := ta.client[alias]
		if topicName == "" {
			return "", errors.NewError(consts.TopicAliasInvalid)
		}
		return topicName, nil
	}
	ta.client[alias] = pubTopicName
	return pubTopicName, nil
}

func (ta *topicAlias) Server(pubTopicName string) (alias uint16, exist bool) {
	if a, ok := ta.server[pubTopicName]; ok {
		return a, true
	}
	ta.serverCurAliasOffset++
	if ta.serverCurAliasOffset >= ta.clientMaxAliasNum {
		ta.serverCurAliasOffset = 1
	}
	ta.server[pubTopicName] = ta.serverCurAliasOffset
	return ta.serverCurAliasOffset, false
}
