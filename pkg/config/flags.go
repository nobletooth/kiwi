package config

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"testing"

	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
)

var configFilePath = flag.String("config_file", "config.txtpb", "Path to the configuration file.")

// InitFlags initializes the flags from the config file specified by the -config_file flag.
// It should be called after defining all flags and before using them.
// Assumes config file doesn't have repeated/map fields. Supports nested messages and oneof blocks only.
func InitFlags() {
	flag.Parse()

	if *configFilePath == "" {
		slog.Info("Config file not specified. Skipping config initialization.")
		return
	}

	// Read config file.
	configFile, err := os.Open(*configFilePath)
	if errors.Is(err, os.ErrNotExist) {
		slog.Warn("Config file does not exist.", "path", *configFilePath, "error", err)
		return
	}
	if err != nil { // If the config file cannot be opened, we skip loading and use default flag values.
		slog.Error("Failed to open config file.", "error", err)
		return
	}
	configBytes, err := io.ReadAll(configFile)
	if err != nil {
		slog.Error("Failed to read config file.", "error", err)
		return
	}
	_ = configFile.Close()

	// Apply configurations.
	conf := new(kiwipb.Config)
	if err := prototext.Unmarshal(configBytes, conf); err != nil {
		slog.Error("Failed to parse config file.", "error", err)
		return
	}
	if err := setConfigFlags(conf); err != nil {
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
