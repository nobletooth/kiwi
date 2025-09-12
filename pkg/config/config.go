// Kiwi uses flags and a single config file for configuration.
// A config file is stored in .txtpb format and contains the values that can be set via flags.

package config

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var configFile = flag.String("config_file", "config.txtpb", "Path to the configuration file.")

// protobufValueToString converts a protobuf field value to its string representation suitable for flag setting.
func protobufValueToString(fd protoreflect.FieldDescriptor, v protoreflect.Value) (string, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10), nil
	case protoreflect.FloatKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32), nil
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64), nil
	case protoreflect.StringKind:
		return v.String(), nil
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes()), nil
	case protoreflect.EnumKind:
		// Use enum name for readability.
		if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name()), nil
		}
		return strconv.FormatInt(int64(v.Enum()), 10), nil
	case protoreflect.MessageKind:
		fullName := string(fd.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			return v.Message().Interface().(*timestamppb.Timestamp).AsTime().Format(time.RFC3339Nano), nil
		case "google.protobuf.Duration":
			return v.Message().Interface().(*durationpb.Duration).AsDuration().String(), nil
		default:
			// If a message itself has a flag_name, we allow its prototext form.
			if proto.HasExtension(fd.Options(), kiwipb.E_FlagName) {
				return prototext.MarshalOptions{Multiline: false}.Format(v.Message().Interface()), nil
			}
			return "", fmt.Errorf("unsupported message leaf: %s", fullName)
		}
	default:
		return "", fmt.Errorf("unsupported kind: %v", fd.Kind())
	}
}

func setConfigFlags(m protoreflect.Message) error {
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// Oneof blocks appear as regular set fields in Range. Lists/maps are not supported by design.
		if fd.IsList() || fd.IsMap() {
			err = fmt.Errorf("repeated/map not supported: %s", fd.FullName())
			return false
		}
		hasFlagName := proto.HasExtension(fd.Options(), kiwipb.E_FlagName)
		// Recurse into nested messages that do not carry a flag_name themselves.
		if fd.Kind() == protoreflect.MessageKind && !hasFlagName {
			if !m.Has(fd) {
				return true
			}
			err = setConfigFlags(v.Message())
			return err == nil
		}
		// If annotated, convert to string and set the target flag.
		if hasFlagName && m.Has(fd) {
			var flagName string
			{ // Get the flag name from the annotation.
				ext := proto.GetExtension(fd.Options(), kiwipb.E_FlagName)
				if extString, ok := ext.(string); ok {
					flagName = extString
				}
			}
			// Convert the protobuf value to string.
			stringValue, convErr := protobufValueToString(fd, v)
			if convErr != nil {
				err = fmt.Errorf("failed to convert %s: %w", fd.FullName(), convErr)
				return false
			}
			// Set the flag value.
			if setErr := flag.Set(flagName, stringValue); setErr != nil {
				err = fmt.Errorf("failed to set flag %s: %w", flagName, setErr)
				return false
			}
		}
		// Skip other fields.
		return true
	})
	return err
}

// InitFlags initializes the flags from the config file specified by the -config_file flag.
// It should be called after defining all flags and before using them.
// Assumes config file doesn't have repeated/map fields. Supports nested messages and oneof blocks only.
func InitFlags() {
	flag.Parse()

	configFile, err := os.Open(*configFile)
	if err != nil { // If the config file cannot be opened, we skip loading and use default flag values.
		slog.Error("Failed to open config file.", "error", err)
		return
	}
	defer func() { _ = configFile.Close() }()

	configBytes, err := io.ReadAll(configFile)
	if err != nil {
		slog.Error("Failed to read config file.", "error", err)
		return
	}

	var conf kiwipb.Config
	if err := prototext.Unmarshal(configBytes, &conf); err != nil {
		slog.Error("Failed to parse config file.", "error", err)
		return
	}

	if err := setConfigFlags(conf.ProtoReflect()); err != nil {
		slog.Error("Failed to set flags from config file.", "error", err)
		return
	}
}

// SetTestFlag sets a flag to a specific value for the duration of the test.
func SetTestFlag(t *testing.T, name, value string) {
	t.Helper()
	flagHolder := flag.Lookup(name)
	require.NotNil(t, flagHolder, "Flag %s not found", name)
	if flagHolder != nil { // Revert the flag value back to its original when the test is done.
		prevValue := flagHolder.Value.String()
		t.Cleanup(func() { require.NoError(t, flag.Set(name, prevValue)) })
	}
	require.NoError(t, flag.Set(name, value))
}
