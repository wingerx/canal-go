package driver

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestSemiSyncIndicatorIsCorrect(t *testing.T) {
	assert.Equal(t, COM_SEMI_MARK, byte(0xef))
}


