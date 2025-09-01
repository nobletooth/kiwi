package utils

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

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
