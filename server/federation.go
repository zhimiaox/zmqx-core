package server

import (
	"context"
	"fmt"
	"strings"

	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"

	"log/slog"

	"github.com/redis/go-redis/v9"
)

type Federation interface {
	Termination(clientID string, code consts.Code)
}

type federationOption func(fed *federation)

type federation struct {
	srv   *server
	sType string
	redis struct {
		rdb           redis.UniversalClient
		clientChannel string
	}
	listenFn func()
}

func federationWithRedis(rdb redis.UniversalClient) federationOption {
	return func(fed *federation) {
		fed.redis.rdb = rdb
		fed.sType = consts.Redis
		fed.redis.clientChannel = consts.GlobalPrefix + ":federation:client-channel"
		fed.listenFn = func() {
			for message := range rdb.Subscribe(context.Background(), fed.redis.clientChannel).Channel() {
				fed.srv.logger.Debug("federation redis Subscribe",
					"is_self", strings.HasPrefix(message.Payload, fed.srv.cfg.Server.NodeID),
					slog.Group("message", "channel", message.Channel, "payload", message.Payload))
				if message.Channel == fed.redis.clientChannel {
					payloadSplit := strings.SplitN(message.Payload, ":", 3)
					if len(payloadSplit) != 3 || payloadSplit[0] == fed.srv.cfg.Server.NodeID {
						continue
					}
					code := consts.AdminAction
					if len(payloadSplit[1]) > 0 {
						code = payloadSplit[1][0]
					}
					fed.unregisterLocalClient(payloadSplit[2], code)
				}
			}
		}
	}
}

func federationWithMemory() federationOption {
	return func(fed *federation) {
		fed.sType = consts.Memory
		fed.listenFn = func() {}
	}
}

func (srv *server) newFederation(opt federationOption) Federation {
	impl := &federation{
		srv: srv,
	}
	opt(impl)
	go impl.listenFn()
	return impl
}

func (fed *federation) Termination(clientID string, code consts.Code) {
	ctx := context.Background()
	if fed.sType == consts.Redis {
		// send a notification to tell other nodes to kick off the client
		payload := fmt.Sprintf("%s:%s:%s", fed.srv.cfg.Server.NodeID, []byte{code}, clientID)
		fed.redis.rdb.Publish(ctx, fed.redis.clientChannel, payload)
		fed.srv.logger.Debug("federation client taken over",
			"channel", fed.redis.clientChannel,
			"payload", fed.srv.cfg.Server.NodeID+":"+clientID)
	}
	fed.unregisterLocalClient(clientID, code)
}

func (fed *federation) unregisterLocalClient(clientID string, code consts.Code) {
	fed.srv.clientsMu.RLock()
	c, ok := fed.srv.clients[clientID]
	fed.srv.clientsMu.RUnlock()
	if ok {
		c.logger.Debug("federation unregister local client", "code", code)
		c.Error(errors.NewError(code))
		<-c.done
	}
}
