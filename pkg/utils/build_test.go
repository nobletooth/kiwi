package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/mod/semver"
)

func TestVersionIsSemantic(t *testing.T) {
	fmt.Println(len(Version))
	assert.Truef(t, semver.IsValid(Version), "Version %s is not a valid semantic version", Version)
}
