// This file contains build information and initialization logic.
// It sets up variables for versioning, commit hash, build time, start time, and hostname.
// CAUTION: This file shouldn't be removed or else flags wouldn't be set properly.

package utils

import (
	"log/slog"
	"strconv"
	"time"
)

var (
	TestMode   string // Should be true when running tests.
	IsTestMode bool
	Version    string
	Commit     string
	BuildTime  string
	StartTime  time.Time
)

func init() {
	StartTime = time.Now()

	// If build info is not set, make that clear.
	if Version == "" {
		Version = "unknown"
	}
	if Commit == "" {
		Commit = "unknown"
	}
	if BuildTime == "" {
		BuildTime = "unknown"
	}
	if len(TestMode) > 0 {
		if isTestMode, err := strconv.ParseBool(TestMode); err == nil {
			IsTestMode = isTestMode
		} else {
			slog.Warn("Failed to parse TestMode build flag, defaulting to false", "error", err)
		}
	}
}
