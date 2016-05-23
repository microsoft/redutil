package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventBuildsString(t *testing.T) {
	e := NewEvent("foo")
	assert.Equal(t, e.sub, "SUBSCRIBE")
	assert.Equal(t, e.unsub, "UNSUBSCRIBE")
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsPattern(t *testing.T) {
	e := NewPatternEvent("foo")
	assert.Equal(t, e.sub, "PSUBSCRIBE")
	assert.Equal(t, e.unsub, "PUNSUBSCRIBE")
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsMultipart(t *testing.T) {
	e := NewEvent("prefix:", String("foo:"), Int(42))
	assert.Equal(t, "prefix:foo:42", e.Name())
	assert.Equal(t, 3, e.Len())

	assert.Equal(t, 42, e.Int(2))
	assert.Equal(t, "foo:", e.String(1))
	assert.Equal(t, "prefix:", e.String(0))

	assert.Equal(t, interface{}(42), e.Get(2))
	assert.Equal(t, interface{}("foo:"), e.Get(1))
}
