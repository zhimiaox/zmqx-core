package memory

import (
	"strings"

	"github.com/zhimiaox/zmqx-core/models"
)

// subscriptionTrie
type subscriptionTrie struct {
	children map[string]*subscriptionTrie
	// clients store non-share subscription
	clients   models.ClientSubscription
	parent    *subscriptionTrie // pointer of parent node
	topicName string
	// shared store shared subscription, key by ShareName
	shared map[string]models.ClientSubscription
}

// newTopicTrie create a new trie tree
func newTopicTrie() *subscriptionTrie {
	return &subscriptionTrie{
		children: make(map[string]*subscriptionTrie),
		clients:  make(models.ClientSubscription),
		shared:   make(map[string]models.ClientSubscription),
	}
}

// newChild create a child node of t
func (t *subscriptionTrie) newChild() *subscriptionTrie {
	n := newTopicTrie()
	n.parent = t
	return n
}

// subscribe add a subscription and return the added node
func (t *subscriptionTrie) subscribe(clientID string, s *models.Subscription) *subscriptionTrie {
	topicSlice := strings.Split(s.TopicFilter, "/")
	var pNode = t
	for _, lv := range topicSlice {
		if _, ok := pNode.children[lv]; !ok {
			pNode.children[lv] = pNode.newChild()
		}
		pNode = pNode.children[lv]
	}
	// shared subscription
	if s.ShareName != "" {
		if pNode.shared[s.ShareName] == nil {
			pNode.shared[s.ShareName] = make(map[string]*models.Subscription)
		}
		pNode.shared[s.ShareName][clientID] = s
	} else {
		// non-shared
		pNode.clients[clientID] = s
	}
	pNode.topicName = s.TopicFilter
	return pNode
}

// find walk through the tire and return the node that represent the topicFilter.
// Return nil if not found
func (t *subscriptionTrie) find(topicFilter string) *subscriptionTrie {
	topicSlice := strings.Split(topicFilter, "/")
	var pNode = t
	for _, lv := range topicSlice {
		if _, ok := pNode.children[lv]; ok {
			pNode = pNode.children[lv]
		} else {
			return nil
		}
	}
	if pNode.topicName == topicFilter {
		return pNode
	}
	return nil
}

// unsubscribe
func (t *subscriptionTrie) unsubscribe(clientID string, topicName string, shareName string) {
	topicSlice := strings.Split(topicName, "/")
	l := len(topicSlice)
	var pNode = t
	for _, lv := range topicSlice {
		if _, ok := pNode.children[lv]; ok {
			pNode = pNode.children[lv]
		} else {
			return
		}
	}
	if shareName != "" {
		if c := pNode.shared[shareName]; c != nil {
			delete(c, clientID)
			if len(pNode.shared[shareName]) == 0 {
				delete(pNode.shared, shareName)
			}
			if len(pNode.shared) == 0 && len(pNode.children) == 0 {
				delete(pNode.parent.children, topicSlice[l-1])
			}
		}
	} else {
		delete(pNode.clients, clientID)
		if len(pNode.clients) == 0 && len(pNode.children) == 0 {
			delete(pNode.parent.children, topicSlice[l-1])
		}
	}

}

// setRs set the node subscription info into rs
func (t *subscriptionTrie) setRs(node *subscriptionTrie, rs models.ClientSubscriptions) {
	for cid, subOpts := range node.clients {
		rs[cid] = append(rs[cid], subOpts)
	}

	for _, c := range node.shared {
		for cid, subOpts := range c {
			rs[cid] = append(rs[cid], subOpts)
		}
	}
}

// matchTopic get all matched topic for given topicSlice, and set into rs
func (t *subscriptionTrie) matchTopic(topicSlice []string, rs models.ClientSubscriptions) {
	endFlag := len(topicSlice) == 1
	if childNode := t.children["#"]; childNode != nil {
		t.setRs(childNode, rs)
	}
	if childNode := t.children["+"]; childNode != nil {
		if endFlag {
			t.setRs(childNode, rs)
			if n := childNode.children["#"]; n != nil {
				t.setRs(n, rs)
			}
		} else {
			childNode.matchTopic(topicSlice[1:], rs)
		}
	}
	if childNode := t.children[topicSlice[0]]; childNode != nil {
		if endFlag {
			t.setRs(childNode, rs)
			if n := childNode.children["#"]; n != nil {
				t.setRs(n, rs)
			}
		} else {
			childNode.matchTopic(topicSlice[1:], rs)
		}
	}
}

// getMatchedTopicFilter return a map key by clientID that contain all matched topic for the given topicName.
func (t *subscriptionTrie) getMatchedTopicFilter(topicName string) models.ClientSubscriptions {
	topicLv := strings.Split(topicName, "/")
	subs := make(models.ClientSubscriptions)
	t.matchTopic(topicLv, subs)
	return subs
}

func (t *subscriptionTrie) preOrderTraverse(fn models.SubscriptionIterateFn) bool {
	if t == nil {
		return false
	}
	if t.topicName != "" {
		for clientID, subOpts := range t.clients {
			if !fn(clientID, subOpts) {
				return false
			}
		}

		for _, c := range t.shared {
			for clientID, subOpts := range c {
				if !fn(clientID, subOpts) {
					return false
				}
			}
		}
	}
	for _, c := range t.children {
		if !c.preOrderTraverse(fn) {
			return false
		}
	}
	return true
}
