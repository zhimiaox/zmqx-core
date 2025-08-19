package memory

import (
	"sync"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Retained = (*retained)(nil)

// retained implement the retain.Store, it use trie tree  to store retain messages .
type retained struct {
	sync.RWMutex
	userTrie   *retainTrie
	systemTrie *retainTrie
}

func (t *retained) getTrie(topicName string) *retainTrie {
	if common.IsSystemTopic(topicName) {
		return t.systemTrie
	}
	return t.userTrie
}

// AddOrReplace add or replace a retain message.
func (t *retained) AddOrReplace(message *models.Message) {
	t.Lock()
	defer t.Unlock()
	t.getTrie(message.Topic).addRetainMsg(message.Topic, message)
}

// Remove  the retain message of the topic name.
func (t *retained) Remove(topicName string) {
	t.Lock()
	defer t.Unlock()
	t.getTrie(topicName).remove(topicName)
}

// GetMatchedMessages returns all messages that match the topic filter.
func (t *retained) GetMatchedMessages(topicFilter string) []*models.Message {
	t.RLock()
	defer t.RUnlock()
	return t.getTrie(topicFilter).getMatchedMessages(topicFilter)
}

func newRetained() persistence.Retained {
	return &retained{
		userTrie:   newRetainTrie(),
		systemTrie: newRetainTrie(),
	}
}
