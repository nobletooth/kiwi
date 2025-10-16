package port

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nobletooth/kiwi/pkg/storage"
	"github.com/tidwall/redcon"
)

var address = flag.String("address", "0.0.0.0:6380", "The ip:port to listen on for Redis protocol.")

// RedisCommand represents a Redis command with its arguments.
type RedisCommand struct {
	command string
	raw     []byte   // All the given command sent over RESP, i.e. GET key.
	args    [][]byte // Only the args sent over, without the command.
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

// SET command:

// parseSetCommand parses an inline-style Redis SET command.
// It supports: SET key value [NX|XX] [GET] [EX s|PX ms|EXAT sec|PXAT ms|KEEPTTL]
// Note: Options must appear in the order shown above for this inline parser.
// For RESP arrays, parse tokens instead of using this regex.
func parseSetCommand(in []byte, now time.Time) (SetCommand, error) {
	// Regex groups:
	// 1:key 2:value 3:NX|XX 4:GET 5:EX|PX|EXAT|PXAT 6:number 7:KEEPTTL
	re := regexp.MustCompile(`(?i)^\s*SET\s+(\S+)\s+(\S+)(?:\s+(NX|XX))?(?:\s+(GET))?(?:(?:\s+(EX|PX|EXAT|PXAT)\s+(\d+))|(?:\s+(KEEPTTL)))?\s*$`)
	m := re.FindSubmatch(in)
	if m == nil {
		return SetCommand{}, fmt.Errorf("invalid SET syntax: %q", strings.TrimSpace(string(in)))
	}

	key, val := m[1], m[2]
	optExist := upperBytes(m[3])
	// m[4] is GET, ignored here since struct has no field for it.
	optTTLKind := upperBytes(m[5])
	num := m[6]
	optKeepTTL := len(m[7]) > 0

	// Existence option.
	var ex existenceCheck
	if bytes.Equal(optExist, []byte("NX")) {
		ex = ifNotExists
	} else if bytes.Equal(optExist, []byte("XX")) {
		ex = ifExists
	} // else: leave zero value; cannot distinguish "no-check" vs NX with given enum

	// TTL calculation.
	var exp time.Time
	if len(optTTLKind) > 0 && optKeepTTL {
		return SetCommand{}, errors.New("KEEPTTL cannot be combined with EX/PX/EXAT/PXAT")
	}
	if len(optTTLKind) > 0 {
		if len(num) == 0 {
			return SetCommand{}, errors.New("missing numeric value for expiration")
		}
		n, err := strconv.ParseInt(string(num), 10, 64)
		if err != nil || n < 0 {
			return SetCommand{}, fmt.Errorf("invalid expiration number: %s", num)
		}
		switch string(optTTLKind) {
		case "EX":
			exp = now.Add(time.Duration(n) * time.Second)
		case "PX":
			exp = now.Add(time.Duration(n) * time.Millisecond)
		case "EXAT":
			exp = time.Unix(n, 0).UTC()
		case "PXAT":
			exp = time.Unix(0, n*int64(time.Millisecond)).UTC()
		default:
			return SetCommand{}, fmt.Errorf("unknown expiration kind: %s", optTTLKind)
		}
	}

	return SetCommand{
		key:        key,
		value:      val,
		expiryTime: exp, // Zero means no expiry unless KEEPTTL keeps an existing one.
		existence:  ex,
		keepTtl:    optKeepTTL,
	}, nil
}

func upperBytes(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	return []byte(strings.ToUpper(string(b)))
}

func handleSetCommand(cmd RedisCommand, store *KiwiStorage) RedisOutput {
	setCommand, err := parseSetCommand(cmd.raw, time.Now())
	if err != nil {
		return writeRedisError(err)
	}
	setResult := store.Set(setCommand)
	if setResult.err != nil {
		return writeRedisError(setResult.err)
	}
	if setResult.hasPreviousValue && setResult.previousValue != nil {
		return writeRedisBytes(setResult.previousValue)
	}
	if !setResult.couldSet {
		return writeRedisNil()
	}
	return writeRedisString("OK")
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
		return handleSetCommand(cmd, rh.store)
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

			// Convert redcon.RedisCommand to RedisCommand.
			redisCmd := RedisCommand{
				command: strings.ToUpper(string(cmd.Args[0])), // Allows case-insensitive commands.
				args:    cmd.Args[1:],                         // Exclude the command itself.
				raw:     cmd.Raw,
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
		slog.Info("Server context cancelled", "err", ctx.Err())
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
