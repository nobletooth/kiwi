package port

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"strings"

	"github.com/nobletooth/kiwi/pkg/storage"
	"github.com/tidwall/redcon"
)

var address = flag.String("address", "0.0.0.0:6380", "The ip:port to listen on for Redis protocol.")

// RedisCommand represents a Redis command with its arguments.
type RedisCommand struct {
	command string
	args    [][]byte
}

// RedisOutput conforms to a real Redis server output on non pub / sub commands.
type RedisOutput struct {
	closeConnection bool    // Closes the connection if true.
	writeNil        bool    // Writes a nil value if true.
	err             *string // Error to return if set.
	writeInt        *int    // Writes an integer value if set.
	writeBytes      []byte  // Writes a string value if set.
}

func closeRedisConnection(msg string) RedisOutput {
	return RedisOutput{writeBytes: []byte(msg), closeConnection: true}
}

func writeRedisNil() RedisOutput {
	return RedisOutput{writeNil: true}
}

func writeRedisInt(i int) RedisOutput {
	return RedisOutput{writeInt: &i}
}

func writeRedisBytes(bytes []byte) RedisOutput {
	return RedisOutput{writeBytes: bytes}
}

func writeRedisString(str string) RedisOutput {
	return RedisOutput{writeBytes: []byte(str)}
}

func writeRedisError(err error) RedisOutput {
	msg := "ERR " + err.Error()
	return RedisOutput{err: &msg}
}

// RedisHandler handles Redis commands using a Kiwi backend.
type RedisHandler struct {
	store *KiwiStorage
}

// NewRedisHandler creates a new RedisHandler.
func NewRedisHandler(store *KiwiStorage) (*RedisHandler, error) {
	if store == nil {
		return nil, errors.New("expected a non-nil store")
	}
	return &RedisHandler{store: store}, nil
}

func (rh *RedisHandler) handle(cmd RedisCommand) RedisOutput {
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
			return writeRedisBytes(value)
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
func RunRedisServer(ctx context.Context, store *KiwiStorage) error {
	if *address == "" {
		return errors.New("expected a non-emptyUnpacked --address flag")
	}

	redisHandler, err := NewRedisHandler(store)
	if err != nil {
		return fmt.Errorf("failed to create a new redis handler: %w", err)
	}

	redisServer := redcon.NewServerNetwork("tcp" /*net*/, *address,
		/*handler*/ func(conn redcon.Conn, cmd redcon.Command) {
			slog.Debug("Handling command.", "cmd", string(cmd.Raw))

			// Convert redcon.Command to RedisCommand.
			redisCmd := RedisCommand{
				command: strings.ToUpper(string(cmd.Args[0])), // Allows case-insensitive commands.
				args:    cmd.Args[1:],                         // Exclude the command itself.
			}
			output := redisHandler.handle(redisCmd)
			if output.closeConnection {
				conn.WriteBulk(output.writeBytes)
				if err := conn.Close(); err != nil {
					slog.Error("failed to close connection", "error", err)
				}
				return
			}
			if output.writeNil {
				conn.WriteNull()
				return
			}
			if output.err != nil {
				conn.WriteError(*output.err)
				return
			}
			if output.writeInt != nil {
				conn.WriteInt(*output.writeInt)
				return
			}
			conn.WriteBulk(output.writeBytes)
		},
		/*accept*/ func(conn redcon.Conn) bool {
			slog.Info("Accepting connection.", "addr", conn.NetConn().RemoteAddr().String())
			return true // Accept all connections.
		},
		/*close*/ func(conn redcon.Conn, err error) {
			// TODO: handle connection errors if needed.
		})

	serverErrSignal := make(chan error, 1)
	go func() {
		slog.Info("Starting Redis server.", "address", *address)
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
