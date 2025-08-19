package server

import (
	"log/slog"

	"github.com/zhimiaox/zmqx-core/config"
)

type Options func(srv *server)

func WithHooks(hooks Hooks) Options {
	return func(srv *server) {
		srv.hooks = hooks
	}
}

func WithHook(hook ...Hook) Options {
	return func(srv *server) {
		srv.hooks = NewHooks(hook...)
	}
}

func WithConfig(cfg *config.Config) Options {
	return func(srv *server) {
		srv.cfg = cfg
	}
}

func WithLogger(l *slog.Logger) Options {
	return func(srv *server) {
		srv.logger = l
	}
}

func WithLoggerHandler(h slog.Handler) Options {
	return func(srv *server) {
		srv.logger = slog.New(h)
	}
}
