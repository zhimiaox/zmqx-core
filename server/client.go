package server

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/common"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/persistence"

	"log/slog"
)

type Client interface {
	// ClientOptions return a reference of ClientOptions. Do not edit.
	// This is mainly used in hooks.
	ClientOptions() *models.ClientOptions
	// SessionInfo return a reference of session information of the client. Do not edit.
	// Session info will be available after the client has passed OnSessionCreated or OnSessionResume.
	SessionInfo() *models.Session
	// Version return the protocol version of the used client.
	Version() packets.Version
	// ConnectedAt returns the connected time
	ConnectedAt() time.Time
	// Connection returns the raw net.Conn
	Connection() net.Conn
	// Error sends a disconnect packet to client, it is used to close v5 client.
	Error(err error)
}

type client struct {
	ctx struct {
		context.Context
		cancel context.CancelFunc
	}
	done   chan struct{}
	regErr chan error

	srv          *server
	rwc          net.Conn
	packetReader *packets.Reader
	packetWriter *packets.Writer
	in           chan packets.Packet
	out          chan packets.Packet

	opts          *models.ClientOptions
	version       packets.Version
	cleanStart    bool
	sessionResume bool
	keepAlive     time.Duration
	// serverReceiveMaximumQuota mqtt v5 server receives max quota
	serverReceiveMaximumQuota uint16
	// serverQuotaMu mqtt v5 quota lock
	serverQuotaMu sync.Mutex

	packetIDLimiter persistence.PacketIDLimiter
	topicAlias      persistence.TopicAliasManager
	queueStore      persistence.Queue
	unackStore      persistence.Unack
	session         *models.Session
	logger          *slog.Logger
}

func (c *client) ClientOptions() *models.ClientOptions {
	return c.opts
}

func (c *client) SessionInfo() *models.Session {
	return c.session
}

func (c *client) Version() packets.Version {
	return c.version
}

func (c *client) ConnectedAt() time.Time {
	return c.session.ConnectedAt
}

func (c *client) Connection() net.Conn {
	return c.rwc
}

func (srv *server) newClient(conn net.Conn) {
	if err := srv.hooks.OnAccept(conn); err != nil {
		_ = conn.Close()
		return
	}
	pool := common.GetIOPool()
	readerBuffer := pool.GetReader(conn, consts.ReadBufferSize)
	writerBuffer := pool.GetWriter(conn, consts.WriteBufferSize)
	c := &client{
		srv:          srv,
		rwc:          conn,
		packetReader: packets.NewReader(readerBuffer),
		packetWriter: packets.NewWriter(writerBuffer),
		in:           make(chan packets.Packet, 8),
		out:          make(chan packets.Packet, 8),
		opts:         &models.ClientOptions{},
		regErr:       make(chan error),
		logger:       srv.logger,
		done:         make(chan struct{}),
	}
	c.ctx.Context, c.ctx.cancel = context.WithCancel(context.Background())
	go c.readLoop()
	go c.writeLoop()
	if c.connectWithTimeout() {
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.pollMessageHandler(wg)
		c.readHandle(wg)
		c.packetIDLimiter.Close()
		c.queueStore.Close()
		wg.Wait()
	}
	// 释放资源
	if c.opts.ClientID != "" {
		c.srv.unregisterClient(c)
	}
	_ = c.rwc.Close()
	close(c.done)
	// 结束销毁资源
	pool.PutReader(readerBuffer)
	pool.PutWriter(writerBuffer)
}

func (c *client) connectWithTimeout() (success bool) {
	ctx, cancel := context.WithTimeout(c.ctx.Context, 10*time.Second)
	connack := &packets.Connack{Code: consts.MalformedPacket}
	defer func() {
		c.write(connack)
		success = connack.Code == consts.Success
		cancel()
	}()
	select {
	case <-ctx.Done():
		return
	case p, ok := <-c.in:
		if !ok || p == nil {
			return
		}
		conn, ok := p.(*packets.Connect)
		if !ok {
			return
		}
		connack = c.connectHandler(ctx, conn)
		if connack.Code != consts.Success {
			return
		}
	}
	c.logger = c.logger.With("client_id", c.opts.ClientID)
	// 持久化上锁，防止session过期任务穿插
	for waitSleep := time.Millisecond; !c.srv.persistence.TryLock(c.opts.ClientID); waitSleep *= 2 {
		select {
		case <-ctx.Done():
			connack.Code = consts.ServerBusy
			c.logger.Warn("client connect with timeout for try persistence lock timeout")
			return
		default:
			c.logger.Debug("connect waiting", "sleep", waitSleep)
			time.Sleep(waitSleep)
		}
	}
	defer c.srv.persistence.Unlock(c.opts.ClientID)
	// 载入注册队列
	c.srv.register <- c
	if err := <-c.regErr; err != nil {
		c.logger.Error("failed register client", "err", err)
		connack.Code = consts.ServerUnavailable
	}
	connack.SessionPresent = c.sessionResume // [MQTT-3.2.2-2]
	return
}

func (c *client) pollMessageHandler(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		elems, err := c.queueStore.ReadInflight(c.opts.MaxInflight)
		if err != nil {
			c.logger.Error("read inflight message for queue store", "err", err)
			c.Error(err)
			return
		}
		if len(elems) == 0 {
			break
		}
		for _, v := range elems {
			switch m := v.MessageWithID.(type) {
			case *models.Publish:
				m.Dup = true
				// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Subscription_Options
				// The server need not use the same set of Subscription Identifiers in the retransmitted PUBLISH packet.
				m.SubscriptionIdentifier = nil
				c.packetIDLimiter.MarkUsedLocked(m.PacketID)
				c.write(models.MessageToPublish(m.Message, c.version))
			case *models.Pubrel:
				c.write(&packets.Pubrel{PacketID: m.PacketID})
			}
		}
	}
	for {
		ids := c.packetIDLimiter.PollPacketIDs(min(c.opts.MaxInflight, 100))
		if ids == nil {
			c.logger.Debug("packet id limiter is close")
			return
		}
		elems, err := c.queueStore.Read(ids)
		if err != nil {
			c.logger.Debug("read message for queue store err", "ids", ids, "err", err)
			c.Error(err)
			return
		}
		used := 0
		for _, v := range elems {
			switch m := v.MessageWithID.(type) {
			case *models.Publish:
				if m.QoS != packets.Qos0 {
					used++
				}
				if packets.IsVersion5(c.version) && m.Message.MessageExpiry != 0 {
					m.Message.MessageExpiry = uint32(time.Since(v.At).Seconds())
				}
				c.write(models.MessageToPublish(m.Message, c.version))
			case *models.Pubrel:
			}
		}
		c.packetIDLimiter.BatchRelease(ids[used:])
	}
}

func (c *client) write(packets packets.Packet) {
	select {
	case <-c.ctx.Done():
		return
	case c.out <- packets:
	}
}

func (c *client) Error(err error) {
	c.logger.Info("client err", "err", err)
	if err == nil {
		err = errors.NewError(consts.UnspecifiedError)
	}
	var codesErr *errors.Error
	if !errors.As(err, &codesErr) {
		codesErr = errors.ErrProtocol
	}
	c.write(&packets.Disconnect{
		Version:    c.version,
		Code:       codesErr.Code,
		Properties: c.properties(codesErr),
	})
}

func (c *client) properties(e *errors.Error) *packets.Properties {
	userProperties := make([]packets.UserProperty, len(e.UserProperties))
	for k, v := range e.UserProperties {
		userProperties[k].K = v.K
		userProperties[k].V = v.V
	}
	return &packets.Properties{
		ReasonString: e.ReasonString,
		User:         userProperties,
	}
}

func (c *client) addServerQuota() {
	c.serverQuotaMu.Lock()
	defer c.serverQuotaMu.Unlock()
	if c.serverReceiveMaximumQuota < c.opts.ReceiveMax {
		c.serverReceiveMaximumQuota++
	}
}

func (c *client) tryDecServerQuota() error {
	c.serverQuotaMu.Lock()
	defer c.serverQuotaMu.Unlock()
	if c.serverReceiveMaximumQuota < 1 {
		c.logger.Error(
			"try dec server quota max exceeded",
			"server_receive_maximum_quota", c.serverReceiveMaximumQuota,
			"ReceiveMax", c.opts.ReceiveMax,
		)
		return errors.NewError(consts.RecvMaxExceeded)
	}
	c.serverReceiveMaximumQuota--
	return nil
}

func (c *client) readLoop() {
	defer func() {
		_ = recover()
		c.ctx.cancel()
		close(c.in)
	}()
	for {
		if c.keepAlive != 0 {
			_ = c.rwc.SetReadDeadline(time.Now().Add(c.keepAlive))
		} else {
			_ = c.rwc.SetReadDeadline(time.Time{}) // 不超时
		}
		packet, err := c.packetReader.ReadPacket()
		if err != nil {
			c.logger.Debug("client package read err", "packet", packet, "err", err)
			return
		}
		if packets.IsVersion5(c.version) {
			if pub, ok := packet.(*packets.Publish); ok && pub.Qos > packets.Qos0 {
				if err = c.tryDecServerQuota(); err != nil {
					c.Error(err)
				}
			}
		}
		select {
		case <-c.ctx.Done():
			return
		case c.in <- packet:
		}
	}
}

func (c *client) writeLoop() {
	defer func() {
		_ = recover()
		c.ctx.cancel()
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case packet := <-c.out:
			if packets.IsVersion5(c.version) {
				switch p := packet.(type) {
				case *packets.Publish:
					if c.opts.ClientTopicAliasMax > 0 {
						// use alias if exist
						alias, ok := c.topicAlias.Server(string(p.TopicName))
						p.Properties.TopicAlias = &alias
						if ok {
							p.TopicName = []byte{}
						}
					}
					c.srv.hooks.OnDelivered(c, p)
				case *packets.Puback, *packets.Pubcomp:
					c.addServerQuota()
				case *packets.Pubrec:
					if p.Code >= consts.UnspecifiedError {
						c.addServerQuota()
					}
				}
			}
			if err := c.packetWriter.WriteAndFlush(packet); err != nil {
				c.logger.Debug("client package write err", "err", err)
				return
			}
			if _, ok := packet.(*packets.Disconnect); ok {
				return
			}
		}
	}
}
