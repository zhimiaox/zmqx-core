package redis

import (
	"bytes"
	"context"
	"strings"

	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

var _ persistence.Retained = (*retained)(nil)

// retained implement the retain.Store, it use trie tree  to store retain messages .
type retained struct {
	rdb    redis.UniversalClient
	logger *slog.Logger
}

func (p *persistenceImpl) newRetained() *retained {
	return &retained{
		rdb:    p.rdb,
		logger: p.logger.With("persistence", "retained"),
	}
}

// AddOrReplace add or replace a retain message.
func (t *retained) AddOrReplace(message *models.Message) {
	ctx := context.Background()
	topicSlice := strings.Split(message.Topic, "/")
	tireKey := &strings.Builder{}
	tireKey.WriteString(retainedTrieKey)
	for i := range topicSlice {
		if i == 0 {
			if err := t.rdb.SAdd(ctx, retainedTrieRootKey, topicSlice[0]).Err(); err != nil {
				t.logger.Error("retained tire root store", "topic_slice", topicSlice[0], "err", err)
				return
			}
		}
		if i+1 == len(topicSlice) {
			break
		}
		if i > 0 {
			tireKey.WriteByte('/')
		}
		tireKey.WriteString(topicSlice[i])
		if err := t.rdb.SAdd(ctx, tireKey.String(), topicSlice[i+1]).Err(); err != nil {
			t.logger.Error("retained tire store err",
				"tire_key", tireKey.String(), "topic_slice", topicSlice[i+1], "err", err,
			)
			return
		}
	}
	b := &bytes.Buffer{}
	models.EncodeMessage(message, b)
	if err := t.rdb.Set(ctx, retainedDataKey+message.Topic, b.String(), 0).Err(); err != nil {
		t.logger.Error("retained data store err", "message", message, "err", err)
	}
}

// Remove  the retain message of the topic name.
func (t *retained) Remove(topicName string) {
	t.rdb.Del(context.Background(), retainedDataKey+topicName)
	lat := strings.LastIndex(topicName, "/")
	parentNode := ""
	node := topicName
	if lat != -1 {
		parentNode = topicName[:lat]
		node = topicName[lat+1:]
	}
	// TODO: 上级空叶子清理(大量topic下会遗留大量空闲的枝杈)
	t.rdb.SRem(context.Background(), retainedTrieKey+parentNode, node)
}

// GetMatchedMessages returns all messages that match the topic filter.
func (t *retained) GetMatchedMessages(topicFilter string) []*models.Message {
	ctx := context.Background()
	res := make([]*models.Message, 0)
	topicFilterSlice := strings.Split(topicFilter, "/")
	matchTopics := make([]string, 0)
	for i, filterNode := range topicFilterSlice {
		if i == 0 {
			if filterNode == "#" {
				for _, key := range t.rdb.Keys(ctx, retainedDataKey+"*").Val() {
					matchTopics = append(matchTopics, strings.TrimPrefix(key, retainedDataKey))
				}
				break
			} else if filterNode == "+" {
				matchTopics = t.rdb.SMembers(ctx, retainedTrieRootKey).Val()
			} else if t.rdb.SIsMember(ctx, retainedTrieRootKey, filterNode).Val() {
				matchTopics = append(matchTopics, filterNode)
			} else {
				break
			}
		} else {
			tempMath := make([]string, 0)
			for i2 := range matchTopics {
				if filterNode == "#" {
					for _, key := range t.rdb.Keys(ctx, retainedDataKey+matchTopics[i2]+"*").Val() {
						tempMath = append(tempMath, strings.TrimPrefix(key, retainedDataKey))
					}
					break
				} else if filterNode == "+" {
					for _, node := range t.rdb.SMembers(ctx, retainedTrieKey+matchTopics[i2]).Val() {
						tempMath = append(tempMath, matchTopics[i2]+"/"+node)
					}
				} else if t.rdb.SIsMember(ctx, retainedTrieKey+matchTopics[i2], filterNode).Val() {
					tempMath = append(tempMath, matchTopics[i2]+"/"+filterNode)
				}
			}
			matchTopics = tempMath
		}
	}
	for _, matchTopic := range matchTopics {
		if data := t.rdb.Get(ctx, retainedDataKey+matchTopic).Val(); len(data) != 0 {
			if msg, err := models.DecodeMessage(bytes.NewBufferString(data)); err == nil && msg != nil {
				res = append(res, msg)
			}
		}
	}
	return res
}
