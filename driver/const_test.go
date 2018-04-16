package driver

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSemiSyncIndicatorIsCorrect(t *testing.T) {
	assert.Equal(t, COM_SEMI_MARK, byte(0xef))
}
