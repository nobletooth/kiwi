package utils

import (
	"flag"
	"log/slog"
	"os"
	"strings"
)

type LogHandlerType string

const (
	HandlerTypeText LogHandlerType = "text"
	HandlerTypeJSON LogHandlerType = "json"
)

type LogLevel string

const (
	LogLevelError LogLevel = "error"
	LogLevelWarn  LogLevel = "warn"
	LogLevelInfo  LogLevel = "info"
	LogLevelDebug LogLevel = "debug"
)

var (
	handlerTypeFlag = flag.String("log_handler_type", string(HandlerTypeJSON), "Log handler type: json/text")
	logLevelFlag    = flag.String("log_level", string(LogLevelInfo), "Log level: debug/info/warn/error")
)

// initLoggingWith configures default logger of slog with given arguments.
func initLoggingWith(handlerType LogHandlerType, logLevel LogLevel) {
	slogLevel := slog.LevelInfo
	switch logLevel {
	case LogLevelDebug:
		slogLevel = slog.LevelDebug
	case LogLevelInfo:
		slogLevel = slog.LevelInfo
	case LogLevelWarn:
		slogLevel = slog.LevelWarn
	case LogLevelError:
		slogLevel = slog.LevelError
	default:
		RaiseInvariant("log", "unsupported_log_level", "Got an unsupported log level.",
			"logLevel", logLevel)
	}

	handlerOptions := slog.HandlerOptions{Level: slogLevel}
	var handler slog.Handler
	switch handlerType {
	case HandlerTypeJSON:
		handler = slog.NewJSONHandler(os.Stdout, &handlerOptions)
	case HandlerTypeText:
		handler = slog.NewTextHandler(os.Stdout, &handlerOptions)
	default:
		RaiseInvariant("log", "unsupported_handler_type", "Got an unsupported handler type.",
			"handlerType", handlerType)
		handler = slog.NewJSONHandler(os.Stdout, &handlerOptions)
	}

	// `SetDefault` happens atomically and doesn't panic when called in multiple goroutines.
	slog.SetDefault(slog.New(handler))
	slog.Debug("Log handler configured successfully.", "type", handlerType, "logLevel", logLevel)
}

// InitLogging configures default logger of slog. Note that this method must be called after flag.Parse().
func InitLogging() {
	initLoggingWith(LogHandlerType(strings.ToLower(*handlerTypeFlag)), LogLevel(strings.ToLower(*logLevelFlag)))
}
