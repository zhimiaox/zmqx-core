package memory

import (
	"strings"

	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
)

// retainTrie
type retainTrie struct {
	children map[string]*retainTrie
	msg      *models.Message
	// pointer of parent node
	parent    *retainTrie
	topicName string
}

// newRetainTrie create a new trie tree
func newRetainTrie() *retainTrie {
	return &retainTrie{
		children: make(map[string]*retainTrie),
	}
}

// newChild create a child node of t
func (t *retainTrie) newChild() *retainTrie {
	n := newRetainTrie()
	n.parent = t
	return n
}

// find walk through the tire and return the node that represent the topicName
// return nil if not found
//
//nolint:unused
func (t *retainTrie) find(topicName string) *retainTrie {
	topicSlice := strings.Split(topicName, "/")
	var pNode = t
	for _, lv := range topicSlice {
		if _, ok := pNode.children[lv]; ok {
			pNode = pNode.children[lv]
		} else {
			return nil
		}
	}
	if pNode.msg != nil {
		return pNode
	}
	return nil
}

// matchTopic walk through the tire and call the fn callback for each message witch match the topic filter.
func (t *retainTrie) matchTopic(topicSlice []string, fn persistence.RetainedIterateFn) {
	endFlag := len(topicSlice) == 1
	switch topicSlice[0] {
	case "#":
		t.preOrderTraverse(fn)
	case "+":
		// match all the current layer
		for _, v := range t.children {
			if endFlag {
				if v.msg != nil {
					fn(v.msg)
				}
			} else {
				v.matchTopic(topicSlice[1:], fn)
			}
		}
	default:
		if n := t.children[topicSlice[0]]; n != nil {
			if endFlag {
				if n.msg != nil {
					fn(n.msg)
				}
			} else {
				n.matchTopic(topicSlice[1:], fn)
			}
		}
	}
}

func (t *retainTrie) getMatchedMessages(topicFilter string) []*models.Message {
	topicLv := strings.Split(topicFilter, "/")
	var rs []*models.Message
	t.matchTopic(topicLv, func(message *models.Message) bool {
		rs = append(rs, message.Copy())
		return true
	})
	return rs
}

// addRetainMsg add a retain message
func (t *retainTrie) addRetainMsg(topicName string, message *models.Message) {
	topicSlice := strings.Split(topicName, "/")
	var pNode = t
	for _, lv := range topicSlice {
		if _, ok := pNode.children[lv]; !ok {
			pNode.children[lv] = pNode.newChild()
		}
		pNode = pNode.children[lv]
	}
	pNode.msg = message
	pNode.topicName = topicName
}

func (t *retainTrie) remove(topicName string) {
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
	pNode.msg = nil
	if len(pNode.children) == 0 {
		delete(pNode.parent.children, topicSlice[l-1])
	}
}

func (t *retainTrie) preOrderTraverse(fn persistence.RetainedIterateFn) bool {
	if t == nil {
		return false
	}
	if t.msg != nil {
		if !fn(t.msg) {
			return false
		}
	}
	for _, c := range t.children {
		if !c.preOrderTraverse(fn) {
			return false
		}
	}
	return true
}
