package common

import (
	"crypto/rand"
	"strings"
)

// ConvertPtr 转换读取指针值
func ConvertPtr[T any](val *T, defaultVal T) T {
	if val == nil {
		return defaultVal
	}
	return *val
}

func Bool2Byte(bo bool) *byte {
	var b byte
	if bo {
		b = 1
	}
	return &b
}

func IsSystemTopic(topicName string) bool {
	return len(topicName) >= 1 && topicName[0] == '$'
}

// SplitTopic returns the shareName and topicFilter of the given topic.
// If the topic is invalid, returns empty strings.
func SplitTopic(topic string) (shareName, topicFilter string) {
	if strings.HasPrefix(topic, "$share/") {
		shared := strings.SplitN(topic, "/", 3)
		if len(shared) < 3 {
			return "", ""
		}
		return shared[1], shared[2]
	}
	return "", topic
}

var nanoIDAlphabet = []rune("_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func NanoID(l ...int) string {
	size := 21
	if len(l) > 0 {
		size = l[0]
	}
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	id := make([]rune, size)
	for i := 0; i < size; i++ {
		id[i] = nanoIDAlphabet[bytes[i]&63]
	}
	return string(id[:size])
}
