package memory

import (
	"strings"
	"sync"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
)

var _ persistence.Subscription = (*subscription)(nil)

// subscription implement the subscription.Interface, it use trie tree to store topics.
type subscription struct {
	sync.RWMutex
	// [clientID][topicFilter]
	userIndex map[string]map[string]*subscriptionTrie
	userTrie  *subscriptionTrie

	// system topic which begin with "$"
	// [clientID][topicFilter]
	systemIndex map[string]map[string]*subscriptionTrie
	systemTrie  *subscriptionTrie

	// shared subscription which begin with "$share"
	// [clientID][shareName/topicFilter]
	sharedIndex map[string]map[string]*subscriptionTrie
	sharedTrie  *subscriptionTrie
}

func (s *subscription) iterateShared(fn models.SubscriptionIterateFn, options models.IterationOptions, index map[string]map[string]*subscriptionTrie, trie *subscriptionTrie) bool {
	// match the specified topicName
	if options.TopicName != "" && options.MatchType == consts.MatchName {
		var shareName string
		var topicFilter string
		if strings.HasPrefix(options.TopicName, "$share/") {
			shared := strings.SplitN(options.TopicName, "/", 3)
			shareName = shared[1]
			topicFilter = shared[2]
		} else {
			return true
		}
		node := trie.find(topicFilter)
		if node == nil {
			return true
		}
		// match the specified topicName and clientID
		if options.ClientID != "" {
			if c := node.shared[shareName]; c != nil {
				if sub, ok := c[options.ClientID]; ok {
					if !fn(options.ClientID, sub) {
						return false
					}
				}
			}
		} else {
			if c := node.shared[shareName]; c != nil {
				for clientID, sub := range c {
					if !fn(clientID, sub) {
						return false
					}
				}
			}
		}
		return true
	}
	if options.TopicName != "" && options.MatchType == consts.MatchFilter {
		node := trie.getMatchedTopicFilter(options.TopicName)
		if node == nil {
			return true
		}
		if options.ClientID != "" {
			for _, v := range node[options.ClientID] {
				if !fn(options.ClientID, v) {
					return false
				}
			}
		} else {
			for clientID, subs := range node {
				for _, v := range subs {
					if !fn(clientID, v) {
						return false
					}
				}
			}
		}
		return true
	}
	// find all subscribed topics under the specified clientID
	if options.ClientID != "" {
		for _, v := range index[options.ClientID] {
			for _, c := range v.shared {
				if sub, ok := c[options.ClientID]; ok {
					if !fn(options.ClientID, sub) {
						return false
					}
				}
			}
		}
		return true
	}
	return trie.preOrderTraverse(fn)
}

func (s *subscription) iterateNonShared(fn models.SubscriptionIterateFn, options models.IterationOptions, index map[string]map[string]*subscriptionTrie, trie *subscriptionTrie) bool {
	// query the specified topicFilter
	if options.TopicName != "" && options.MatchType == consts.MatchName {
		// find nodes according to the specified topic name
		node := trie.find(options.TopicName)
		if node == nil {
			return true
		}
		if options.ClientID != "" {
			// match the specified topicName and clientID
			if sub, ok := node.clients[options.ClientID]; ok {
				if !fn(options.ClientID, sub) {
					return false
				}
			}

			for _, v := range node.shared {
				if sub, ok := v[options.ClientID]; ok {
					if !fn(options.ClientID, sub) {
						return false
					}
				}
			}

		} else {
			// match topic name without clientID
			for clientID, sub := range node.clients {
				if !fn(clientID, sub) {
					return false
				}
			}
			for _, c := range node.shared {
				for clientID, sub := range c {
					if !fn(clientID, sub) {
						return false
					}
				}
			}

		}
		return true
	}
	if options.TopicName != "" && options.MatchType == consts.MatchFilter {
		node := trie.getMatchedTopicFilter(options.TopicName)
		if node == nil {
			return true
		}
		if options.ClientID != "" {
			for _, v := range node[options.ClientID] {
				if !fn(options.ClientID, v) {
					return false
				}
			}
		} else {
			for clientID, subs := range node {
				for _, v := range subs {
					if !fn(clientID, v) {
						return false
					}
				}
			}
		}
		return true
	}
	// find all topics under the specified clientID
	if options.ClientID != "" {
		for _, v := range index[options.ClientID] {
			sub := v.clients[options.ClientID]
			if !fn(options.ClientID, sub) {
				return false
			}
		}
		return true
	}
	return trie.preOrderTraverse(fn)

}

// IterateLocked is the non thread-safe version of Iterate
func (s *subscription) IterateLocked(fn models.SubscriptionIterateFn, options models.IterationOptions) {
	if options.Type&consts.TypeShared == consts.TypeShared {
		if !s.iterateShared(fn, options, s.sharedIndex, s.sharedTrie) {
			return
		}
	}
	if options.Type&consts.TypeNonShared == consts.TypeNonShared {
		// The Server MUST NOT match Topic Filters starting with a wildcard character (# or +) with Topic Names beginning with a $ character [MQTT-4.7.2-1]
		if !(options.TopicName != "" && common.IsSystemTopic(options.TopicName)) {
			if !s.iterateNonShared(fn, options, s.userIndex, s.userTrie) {
				return
			}
		}
	}
	if options.Type&consts.TypeSYS == consts.TypeSYS {
		if options.TopicName != "" && !common.IsSystemTopic(options.TopicName) {
			return
		}
		if !s.iterateNonShared(fn, options, s.systemIndex, s.systemTrie) {
			return
		}
	}
}
func (s *subscription) Iterate(fn models.SubscriptionIterateFn, options models.IterationOptions) {
	s.RLock()
	defer s.RUnlock()
	s.IterateLocked(fn, options)
}

// SubscribeLocked is the non thread-safe version of Subscribe
func (s *subscription) SubscribeLocked(clientID string, subscriptions ...*models.Subscription) []models.SubscribeResult {
	var node *subscriptionTrie
	var index map[string]map[string]*subscriptionTrie
	rs := make([]models.SubscribeResult, len(subscriptions))
	for k, sub := range subscriptions {
		topicName := sub.TopicFilter
		rs[k].Subscription = sub
		if sub.ShareName != "" {
			node = s.sharedTrie.subscribe(clientID, sub)
			index = s.sharedIndex
		} else if common.IsSystemTopic(topicName) {
			node = s.systemTrie.subscribe(clientID, sub)
			index = s.systemIndex
		} else {
			node = s.userTrie.subscribe(clientID, sub)
			index = s.userIndex
		}
		if index[clientID] == nil {
			index[clientID] = make(map[string]*subscriptionTrie)
		}
		if _, ok := index[clientID][topicName]; ok {
			rs[k].AlreadyExisted = true
		}
		index[clientID][topicName] = node
	}
	return rs
}

// Subscribe add subscriptions for the client
func (s *subscription) Subscribe(clientID string, subscriptions ...*models.Subscription) ([]models.SubscribeResult, error) {
	s.Lock()
	defer s.Unlock()
	return s.SubscribeLocked(clientID, subscriptions...), nil
}

// UnsubscribeLocked is the non thread-safe version of Unsubscribe
func (s *subscription) UnsubscribeLocked(clientID string, topics ...string) {
	var index map[string]map[string]*subscriptionTrie
	var topicTrie *subscriptionTrie
	for i := range topics {
		shareName, topic := common.SplitTopic(topics[i])
		if shareName != "" {
			topicTrie = s.sharedTrie
			index = s.sharedIndex
		} else if common.IsSystemTopic(topic) {
			index = s.systemIndex
			topicTrie = s.systemTrie
		} else {
			index = s.userIndex
			topicTrie = s.userTrie
		}
		if _, ok := index[clientID]; ok {
			delete(index[clientID], topic)
		}
		topicTrie.unsubscribe(clientID, topic, shareName)
	}
}

// Unsubscribe remove subscriptions for the client
func (s *subscription) Unsubscribe(clientID string, topics ...string) error {
	s.Lock()
	defer s.Unlock()
	s.UnsubscribeLocked(clientID, topics...)
	return nil
}

func (s *subscription) unsubscribeAll(index map[string]map[string]*subscriptionTrie, clientID string) {
	for topicName, node := range index[clientID] {
		delete(node.clients, clientID)
		if len(node.clients) == 0 && len(node.children) == 0 {
			ss := strings.Split(topicName, "/")
			delete(node.parent.children, ss[len(ss)-1])
		}
	}
	delete(index, clientID)
}

// UnsubscribeAllLocked is the non thread-safe version of UnsubscribeAll
func (s *subscription) UnsubscribeAllLocked(clientID string) {
	s.unsubscribeAll(s.userIndex, clientID)
	s.unsubscribeAll(s.systemIndex, clientID)
	s.unsubscribeAll(s.sharedIndex, clientID)
}

// UnsubscribeAll delete all subscriptions of the client
func (s *subscription) UnsubscribeAll(clientID string) error {
	s.Lock()
	defer s.Unlock()
	// user topics
	s.UnsubscribeAllLocked(clientID)
	return nil
}

// getMatchedTopicFilter return a map key by clientID that contain all matched topic for the given topicName.
//
//nolint:unused
func (s *subscription) getMatchedTopicFilter(topicName string) models.ClientSubscriptions {
	// system topic
	if common.IsSystemTopic(topicName) {
		return s.systemTrie.getMatchedTopicFilter(topicName)
	}
	return s.userTrie.getMatchedTopicFilter(topicName)
}

// newSubscription create a new subscription instance
func newSubscription() *subscription {
	return &subscription{
		userIndex: make(map[string]map[string]*subscriptionTrie),
		userTrie:  newTopicTrie(),

		systemIndex: make(map[string]map[string]*subscriptionTrie),
		systemTrie:  newTopicTrie(),

		sharedIndex: make(map[string]map[string]*subscriptionTrie),
		sharedTrie:  newTopicTrie(),
	}
}
