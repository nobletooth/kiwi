// Package invariant introduces a way to handle unexpected bugs / conditions in code.
// Invariants are conditions in code that must be true; otherwise, there is a bug in code.
// Think of what you'd `panic()` on (equivalent to `assert` in other languages),
// but you don't want to crash the server just because of that violation. If an invariant is violated,
// a log error is recorded, and a monitoring counter is incremented that will trigger an alert.
// Bear in mind that it is still up to you (the caller) to handle the erroneous case in your code and, for example,
// do an early return and skip the following computations.
//
// Do not use invariants for conditions that depend on external factors; for example,
// failing to read from Redis should not trigger an invariant violation.
// But reading some invalid value that our other pieces of code should not have produced could be an invariant
// violation. Or if a function guarantees it always returns a non-empty slice, that's a good candidate for an invariant.
//
// Invariants are also a great source of documenting code. Every time a piece of code makes an assumption (e.g.,
// `x` is non-nil or `y` is positive because another piece of code assigns it as such),
// adding an invariant will make that assumption explicit to the reader.
//
// The invariant package is here to allow you to write defensive code. Use it as much as possible.

package utils

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promclient "github.com/prometheus/client_model/go"
)

var invariantsMetric = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "invariants_total",
	Help: "The total number of invariant violations",
}, []string{
	"module", // The module in which this invariant occurred.
	"type",   // The type of the invariant that occurred.
})

func RaiseInvariant(module, invariantType, msg string, args ...any) {
	invariantsMetric.WithLabelValues(module, invariantType).Inc()
	slog.With("invariant", invariantType, "module", module).Error(msg, args...)
	if IsTestMode {
		panic("invariant violated: " + invariantType)
	}
}

// GetMetricValue returns the current value of invariant metric with labels `invariantLabel` and `owner`.
func GetMetricValue(module, invariantType string) int {
	var metric = &promclient.Metric{}
	if err := invariantsMetric.WithLabelValues(module, invariantType).Write(metric); err != nil {
		slog.Error(err.Error())
		return 0
	}
	return int(metric.Counter.GetValue())
}
