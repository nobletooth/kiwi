package port

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"

	"github.com/nobletooth/kiwi/pkg/storage"
	"github.com/tidwall/redcon"
)

const RedisOk = "OK"

var address = flag.String("address", ":6380", "The ip:port to listen on for Redis protocol.")

// redisCommand represents a Redis command with its arguments.
type redisCommand struct {
	command string
	args    []string
}

// redisOutput conforms to a real Redis server output on non pub / sub commands.
type redisOutput struct {
	closeConnection bool    // Closes the connection if true.
	writeNil        bool    // Writes a nil value if true.
	err             *string // Error to return if set.
	writeInt        *int    // Writes an integer value if set.
	writeString     string  // Writes a string value if set.
}

func closeRedisConnection(msg string) redisOutput {
	return redisOutput{writeString: msg, closeConnection: true}
}

func writeRedisNil() redisOutput {
	return redisOutput{writeNil: true}
}

func writeRedisInt(i int) redisOutput {
	return redisOutput{writeInt: &i}
}

func writeRedisString(s string) redisOutput {
	return redisOutput{writeString: s}
}

func writeRedisError(err error) redisOutput {
	msg := "ERR " + err.Error()
	return redisOutput{err: &msg}
}

type redisHandler struct { // Implements RedisHandlerInterface.
	store storage.KeyValueHolder
}

// newRedisHandler creates a new redisHandler.
func newRedisHandler(store storage.KeyValueHolder) (*redisHandler, error) {
	if store == nil {
		return nil, errors.New("expected a non-nil storage")
	}
	return &redisHandler{store: store}, nil
}

func (rh *redisHandler) handle(cmd redisCommand) redisOutput {
	switch cmd.command {
	case "PING":
		return writeRedisString("PONG")
	case "QUIT":
		return closeRedisConnection("OK")
	case "SET":
		if len(cmd.args) != 2 {
			return writeRedisError(errors.New("ERR wrong number of arguments for 'SET' command"))
		}
		key, value := cmd.args[0], cmd.args[1]
		if err := rh.store.Set(key, value); err != nil {
			return writeRedisError(err)
		}
		return writeRedisString("OK")
	case "GET":
		if len(cmd.args) != 1 {
			return writeRedisError(errors.New("wrong number of arguments for 'get' command"))
		}
		key := cmd.args[0]
		if value, err := rh.store.Get(key); errors.Is(err, storage.ErrKeyNotFound) {
			return writeRedisNil()
		} else if err != nil {
			return writeRedisError(err)
		} else {
			return writeRedisString(value)
		}
	case "DEL":
		if len(cmd.args) < 1 {
			return writeRedisError(errors.New("wrong number of arguments for 'DEL' command"))
		}
		deletedCount := 0
		for _, key := range cmd.args {
			if err := rh.store.Delete(key); err == nil {
				deletedCount++
			}
		}
		return writeRedisInt(deletedCount)
	default:
		return writeRedisError(fmt.Errorf("unknown command '%s'", cmd.command))
	}
}

// RunRedisServer starts a Redis protocol server that interacts with the provided KeyValueHolder storage.
func RunRedisServer(ctx context.Context, store storage.KeyValueHolder) error {
	if *address == "" {
		return errors.New("expected a non-empty --address flag")
	}

	redisHandler, err := newRedisHandler(store)
	if err != nil {
		return fmt.Errorf("failed to create a new redis handler: %w", err)
	}

	redisServer := redcon.NewServerNetwork("tcp" /*net*/, *address,
		/*handler*/ func(conn redcon.Conn, cmd redcon.Command) {
			// Convert redcon.Command to redisCommand.
			command := redisCommand{command: string(cmd.Args[0]), args: make([]string, len(cmd.Args)-1)}
			for i := 1; i < len(cmd.Args); i++ {
				command.args[i-1] = string(cmd.Args[i])
			}
			output := redisHandler.handle(command)
			if output.closeConnection {
				conn.WriteString(output.writeString)
				if err := conn.Close(); err != nil {
					slog.Error("failed to close connection", "error", err)
				}
				return
			}
		},
		/*accept*/ func(conn redcon.Conn) bool {
			return true // Accept all connections.
		},
		/*close*/ func(conn redcon.Conn, err error) {
			// TODO: handle connection errors if needed.
		})

	serverErrSignal := make(chan error, 1)
	go func() {
		if err := redisServer.ListenAndServe(); err != nil {
			serverErrSignal <- err
		}
		close(serverErrSignal)
	}()

	select {
	case <-ctx.Done():
		serverErr := redisServer.Close()
		storeErr := store.Close()
		if exitErr := errors.Join(serverErr, storeErr); exitErr != nil {
			return fmt.Errorf("failed to close kiwi: %w", exitErr)
		}
	case err := <-serverErrSignal:
		return fmt.Errorf("redis server stopped unexpectedly: %w", err)
	}

	return nil // Exited with no errors.
}
