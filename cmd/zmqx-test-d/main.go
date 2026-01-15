package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/zhimiaox/zmqx-core/config"
	"github.com/zhimiaox/zmqx-core/errors"
	"github.com/zhimiaox/zmqx-core/models"
	"github.com/zhimiaox/zmqx-core/packets"
	"github.com/zhimiaox/zmqx-core/server"
)

var configFile = flag.String("c", "config.toml", "config file path")

func main() {
	flag.Parse()
	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		panic(err)
	}
	cfg.Server.Debug = os.Getenv("ZMQX_DEBUG") == "true"
	logLevel := new(slog.LevelVar)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	slog.Info("服务启动中..", "NODE_ID", cfg.Server.NodeID, "DEBUG", cfg.Server.Debug)
	if cfg.Server.Debug {
		logLevel.Set(slog.LevelDebug)
		go func() {
			if err = http.ListenAndServe(":6060", nil); err != nil {
				slog.Error("debug listen", "err", err)
			}
		}()
	}
	srv := server.New(
		server.WithConfig(cfg),
		server.WithLogger(slog.Default()),
		// 设置部分hooks钩子
		server.WithHook(
			server.WithOnBasicAuth(func(conn *packets.Connect, authOpts *models.AuthOptions) error {
				return errors.New("ssss")
			}),
			server.WithOnEnhancedAuth(func(conn *packets.Connect, authOpts *models.AuthOptions) (*models.AuthResponse, error) {
				var n, r string
				for _, v := range strings.Split(string(conn.Properties.AuthData), ",") {
					if strings.HasPrefix(v, "n=") {
						n = v[len("n="):]
					}
					if strings.HasPrefix(v, "r=") {
						r = v[len("r="):]
					}
				}
				return &models.AuthResponse{
					Continue: true,
					// r=<client_nonce+server_nonce>,s=<salt>,i=<iteration_count>
					AuthData: []byte(fmt.Sprintf("r=%s%s,s=<salt>,i=1", r, n)),
				}, nil
			}),
			server.WithOnAuth(func(client server.Client, req *models.AuthRequest) (*models.AuthResponse, error) {
				// c=biws,r=<combined_nonce>,p=<client_proof>
				return &models.AuthResponse{
					Continue: false,
					AuthData: []byte("v=<server_signature>"),
				}, errors.New("auth fail")
			}),
			server.WithOnSubscribe(func(client server.Client, req *models.SubscribeRequest) error {
				return nil
			}),
			server.WithOnMsgArrived(func(client server.Client, message *models.Message) error {
				return nil
			}),
			// server.WithOnSubscribe(func(client server.Client, req *models.SubscribeRequest) error {
			//	for _, topic := range req.Subscribe.Topics {
			//		if topic.Name == "test/nosubscribe" {
			//			return errors.New("not access")
			//		}
			//	}
			//	return nil
			// }),
		),
	)
	srv.Start()
	slog.Info("signal received, server closed.", "signal", WaitForSignal())
	srv.Stop()
}

func WaitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
