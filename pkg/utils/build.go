// This file contains build information and initialization logic.
// It sets up variables for versioning, commit hash, build time, start time, and hostname.
// CAUTION: This file shouldn't be removed or else flags wouldn't be set properly.

package utils

import (
	"time"
)

var (
	Test      bool // Should be true when running tests.
	Version   string
	Commit    string
	BuildTime string
	StartTime time.Time
)

func init() {
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
	StartTime = time.Now()
}
