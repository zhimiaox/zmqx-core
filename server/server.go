package server

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/zhimiaox/zmqx-core/config"
	"github.com/zhimiaox/zmqx-core/consts"
	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/persistence"
	pmemory "github.com/zhimiaox/zmqx-core/persistence/memory"
	predis "github.com/zhimiaox/zmqx-core/persistence/redis"

	"log/slog"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// Publisher provides the ability to Publish messages to the broker.
type Publisher interface {
	// Publish  a message to broker.
	// Calling this method will not trigger OnMsgArrived hook.
	Publish(message *models.Message)
}

type Server interface {
	Start()
	Stop()
	SetHooks(hooks Hooks)
	Termination(clientID string)
	Publisher
}

type server struct {
	onceStop   sync.Once
	stopping   bool
	cfg        *config.Config
	logger     *slog.Logger
	hooks      Hooks
	federation Federation

	wsListener  *http.Server
	tcpListener net.Listener

	clientsMu sync.RWMutex
	clients   map[string]*client
	register  chan *client

	persistence persistence.Persistence
}

func New(opts ...Options) Server {
	srv := &server{
		clients:  make(map[string]*client),
		register: make(chan *client),
	}
	for _, fn := range opts {
		fn(srv)
	}
	if srv.hooks == nil {
		srv.hooks = NewHooks()
	}
	if srv.cfg == nil {
		srv.cfg = config.New()
	}
	if srv.logger == nil {
		logLevel := new(slog.LevelVar)
		srv.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
		if srv.cfg.Server.Debug {
			logLevel.Set(slog.LevelDebug)
		}
	}
	switch srv.cfg.Server.Persistence.Type {
	case consts.Memory:
		srv.persistence = pmemory.New(srv.cfg, srv.logger)
		srv.federation = srv.newFederation(federationWithMemory())
	case consts.Redis:
		rdb := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    srv.cfg.Server.Persistence.Redis.Addr,
			Password: srv.cfg.Server.Persistence.Redis.Password, // 没有密码，默认值
			DB:       srv.cfg.Server.Persistence.Redis.Database, // 默认DB 0
		})
		srv.persistence = predis.New(rdb, srv.cfg, srv.logger)
		srv.federation = srv.newFederation(federationWithRedis(rdb))
	}
	// session expiration event
	srv.persistence.Session().Event(func(clientID string) {
		if !srv.persistence.TryLock(clientID) {
			srv.logger.Warn("session tw handler clean session busy for lock")
			return
		}
		defer srv.persistence.Unlock(clientID)
		oldSession, err := srv.persistence.Session().Get(clientID)
		if err != nil {
			srv.logger.Error("session info get err", "client_id", clientID, "err", err)
			return
		}
		if oldSession == nil || !oldSession.IsExpired() {
			srv.logger.Warn(
				"the session information is not found or has not expired, clean up the task and jump out",
				"client_id", clientID,
			)
			return
		}
		srv.persistence.DestroyClient(clientID)
		srv.hooks.OnSessionTerminated(clientID)
		srv.logger.Info("session destroyed", "client_id", clientID)
	})
	// will message event
	srv.persistence.Will().Event(func(willMsg *models.WillMsg) {
		srv.hooks.OnWillPublish(willMsg)
		srv.deliver(willMsg.ClientID, willMsg.Msg)
		srv.hooks.OnWillPublished(willMsg)
		srv.logger.Info("willing message success", "client_id", willMsg.ClientID, "topic", willMsg.Msg.Topic)
	})
	// mqtt client register event
	go func() {
		for c := range srv.register {
			c.regErr <- srv.registerClient(c)
		}
	}()
	return srv
}

func (srv *server) Start() {
	srv.onceStop = sync.Once{}
	srv.stopping = false
	if conf := srv.cfg.Server.TCP; conf != nil {
		go srv.tcpListen(conf)
	}
	if conf := srv.cfg.Server.Websocket; conf != nil {
		go srv.wsListen(conf)
	}
}

func (srv *server) Stop() {
	srv.onceStop.Do(func() {
		srv.stopping = true
		for len(srv.clients) > 0 {
			if srv.clientsMu.TryRLock() {
				for _, c := range srv.clients {
					c.Error(errors.NewError(consts.ServerMoved))
				}
				srv.clientsMu.RUnlock()
			}
			srv.logger.Info("stopping waiting...")
			time.Sleep(time.Second)
		}
		if srv.wsListener != nil {
			if err := srv.wsListener.Close(); err != nil {
				srv.logger.Error("ws listener close", "err", err)
			} else {
				srv.logger.Info("ws listener closed")
			}
		}
		if srv.tcpListener != nil {
			if err := srv.tcpListener.Close(); err != nil {
				srv.logger.Error("tcp listener close", "err", err)
			} else {
				srv.logger.Info("tcp listener closed")
			}
		}
		srv.logger.Info("server stopped")
	})
}

func (srv *server) SetHooks(hooks Hooks) {
	srv.hooks = hooks
}

func (srv *server) Termination(clientID string) {
	srv.federation.Termination(clientID, consts.ServerMoved)
}

func (srv *server) Publish(message *models.Message) {
	srv.deliver("", message)
}

// 已经判断是成功了，注册
func (srv *server) registerClient(c *client) error {
	clientID := c.opts.ClientID
	c.logger.Debug("client register starting")
	srv.federation.Termination(clientID, consts.SessionTakenOver)
	oldSession, err := srv.persistence.Session().Get(clientID)
	if err != nil {
		slog.Error("client old session get failed", "err", err)
		return err
	}
	// remove session
	if err = srv.persistence.Session().Remove(clientID); err != nil {
		slog.Error("client old session remove failed", "err", err)
	}
	// remove willMessage
	srv.persistence.Will().Remove(clientID)
	if c.cleanStart {
		c.logger.Debug("client clean start")
		// clean up previous subscriptions
		if err = srv.persistence.Subscription().UnsubscribeAll(clientID); err != nil {
			slog.Error("client unsubscribe all for clean start", "err", err)
		}
	}
	c.sessionResume = !c.cleanStart && oldSession != nil && !oldSession.IsExpired()
	c.logger.Debug("session and will interval",
		"will_delay_interval", c.session.WillDelayInterval,
		"session_expiry_interval", c.session.ExpiryInterval,
		"opts_session_expiry", c.opts.SessionExpiry)
	// packet id limiter init
	c.packetIDLimiter = persistence.NewPacketIDLimiter(c.opts.MaxInflight)
	// unack init or recovery
	c.unackStore = srv.persistence.Unack(clientID)
	if err = c.unackStore.Init(c.cleanStart); err != nil {
		slog.Error("client unack store init failed", "err", err)
		return err
	}
	// mess queue init or recovery
	c.queueStore = srv.persistence.Queue(clientID)
	if err = c.queueStore.Init(&persistence.QueueInitOptions{
		CleanStart:     c.cleanStart,
		Version:        c.version,
		ReadBytesLimit: c.opts.ClientMaxPacketSize,
		OnMsgDropped:   srv.hooks.OnMsgDropped,
	}); err != nil {
		slog.Error("client queue store init failed", "err", err)
		return err
	}
	// topic alias init
	c.topicAlias = persistence.NewTopicAliasManager(c.opts.ClientTopicAliasMax, c.opts.ServerTopicAliasMax)

	srv.clientsMu.Lock()
	srv.clients[clientID] = c
	srv.clientsMu.Unlock()

	srv.hooks.OnConnected(c)
	if c.sessionResume {
		srv.hooks.OnSessionResumed(c)
	} else {
		srv.hooks.OnSessionCreated(c)
	}
	c.logger.Info("client register")
	return nil
}

func (srv *server) unregisterClient(c *client) {
	if c.session != nil {
		// need to send willMessage
		if c.session.Will != nil {
			will := &models.WillMsg{
				ClientID: c.opts.ClientID,
				After:    time.Duration(min(c.session.WillDelayInterval, c.session.ExpiryInterval)) * time.Second,
				Msg:      c.session.Will.Copy(),
			}
			srv.persistence.Will().Add(will)
			c.logger.Info("遗嘱消息入列", "after", will.After, "topic", will.Msg.Topic, "qos", will.Msg.QoS, "payload", string(will.Msg.Payload))
		}
		c.session.ExpiryAt = time.Now().Add(time.Duration(c.session.ExpiryInterval) * time.Second)
		if err := srv.persistence.Session().Set(c.session); err != nil {
			c.logger.Error("unregister client reset session info", "err", err)
		}
		c.logger.Debug("unregister client session info",
			"will_delay_interval", c.session.WillDelayInterval,
			"expiry_interval", c.session.ExpiryInterval,
			"has_will", c.session.Will != nil)
	}
	srv.clientsMu.Lock()
	delete(srv.clients, c.opts.ClientID)
	srv.clientsMu.Unlock()

	srv.hooks.OnClosed(c.opts.ClientID)
	c.logger.Info("client unregister")
}

func (srv *server) wsListen(conf *config.WebsocketListen) {
	defer srv.Stop()
	var defaultUpgrade = &websocket.Upgrader{
		ReadBufferSize:  consts.ReadBufferSize,
		WriteBufferSize: consts.WriteBufferSize,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		Subprotocols: []string{"mqtt"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(conf.Path, func(w http.ResponseWriter, r *http.Request) {
		if srv.stopping || !websocket.IsWebSocketUpgrade(r) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("0.0?"))
			return
		}
		c, err := defaultUpgrade.Upgrade(w, r, nil)
		if err != nil {
			srv.logger.Error("websocket upgrade", "err", err)
			return
		}
		srv.newClient(&models.WsConn{Conn: c.NetConn(), W: c})
	})
	srv.wsListener = &http.Server{
		Addr:           conf.Listen,
		Handler:        mux,
		MaxHeaderBytes: 1 << 20,
	}
	srv.logger.Info("websocket listen running", "listen", conf.Listen)
	var err error
	if conf.TLS != nil {
		err = srv.wsListener.ListenAndServeTLS(conf.TLS.Cert, conf.TLS.Key)
	} else {
		err = srv.wsListener.ListenAndServe()
	}
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) && srv.stopping {
			return
		}
		srv.logger.Error("ws listen and serve", "err", err)
	}
}

func (srv *server) tcpListen(conf *config.TCPListen) {
	defer srv.Stop()
	var err error
	if conf.TLS != nil {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(conf.TLS.Cert, conf.TLS.Key)
		if err != nil {
			srv.logger.Error("LoadX509KeyPair", "err", err)
			return
		}
		srv.tcpListener, err = tls.Listen("tcp", conf.Listen, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	} else {
		srv.tcpListener, err = net.Listen("tcp", conf.Listen)
	}
	if err != nil {
		srv.logger.Error("tcp listen", "err", err)
		return
	}
	srv.logger.Info("tcp listen running", "listen", conf.Listen)
	for {
		conn, err := srv.tcpListener.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			break
		}
		if srv.stopping {
			break
		}
		go srv.newClient(conn)
	}
}
