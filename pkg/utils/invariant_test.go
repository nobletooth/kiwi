package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRaiseInvariant(t *testing.T) {
	invariantsMetric.Reset() // Reset the metric to ensure a clean state for the test
	RaiseInvariant("invariant", "test", "This is a test invariant violation")
	gotInvariants := GetMetricValue("invariant" /*module*/, "test" /*invariantType*/)
	assert.Equal(t, 1, gotInvariants)
}
