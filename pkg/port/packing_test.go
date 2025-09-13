package port

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnpackedValue(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		unpacked        unpackedValue
		shouldBeExpired bool
	}{
		{
			name:            "tombstone",
			unpacked:        tombstoneUnpacked,
			shouldBeExpired: false,
		},
		{
			name:            "simple",
			unpacked:        unpackedValue{value: []byte("value")},
			shouldBeExpired: false,
		},
		{
			name:            "expirable",
			unpacked:        unpackedValue{opt: Expirable, value: []byte("v"), expiry: time.Now().Add(-1 * time.Hour)},
			shouldBeExpired: true, // Expired one hour ago.
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			packed := testCase.unpacked.pack()
			unpackedAgain, err := unpack(packed)
			assert.NoError(t, err)
			if testCase.unpacked.opt.Is(Expirable) {
				assert.Equal(t, testCase.unpacked.opt, unpackedAgain.opt)
				assert.Equal(t, testCase.unpacked.value, unpackedAgain.value)
				assert.Equal(t, testCase.unpacked.expiry.UnixNano(), unpackedAgain.expiry.UnixNano())
			} else {
				assert.Equal(t, testCase.unpacked, unpackedAgain)
			}
			assert.Equal(t, testCase.shouldBeExpired, unpackedAgain.isExpired())
		})
	}
}
