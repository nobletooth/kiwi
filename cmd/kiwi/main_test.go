package main

import (
	"testing"

	"github.com/nobletooth/kiwi/pkg/config"
)

func TestFlagsAreRegisteredInConfig(t *testing.T) {
	unregisteredFlags := config.CollectUnregisteredFlags()
	if len(unregisteredFlags) != 0 {
		t.Fail()
		for _, flagErr := range unregisteredFlags {
			t.Error(flagErr)
		}
	}
}
