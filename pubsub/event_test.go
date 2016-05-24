package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventTypes(t *testing.T) {
	assert.Equal(t, "SUBSCRIBE", PlainEvent.SubCommand())
	assert.Equal(t, "UNSUBSCRIBE", PlainEvent.UnsubCommand())
	assert.Equal(t, "PSUBSCRIBE", PatternEvent.SubCommand())
	assert.Equal(t, "PUNSUBSCRIBE", PatternEvent.UnsubCommand())
}

func TestEventBuildsString(t *testing.T) {
	e := NewEvent("foo")
	assert.Equal(t, PlainEvent, e.Type())
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsPattern(t *testing.T) {
	e := NewPatternEvent("foo")
	assert.Equal(t, PatternEvent, e.Type())
	assert.Equal(t, e.Name(), "foo")
}

func TestEventBuildsMultipart(t *testing.T) {
	e := NewEvent("prefix:", String("foo:"), Int(42))
	assert.Equal(t, "prefix:foo:42", e.Name())
	assert.Equal(t, 3, e.Len())

	assert.Equal(t, "prefix:", e.Get(0).String())
	id, _ := e.Get(2).Int()
	assert.Equal(t, "foo:", e.Get(1).String())
	assert.Equal(t, 42, id)
}

func TestEventReturnsZeroOnDNE(t *testing.T) {
	assert.True(t, NewEvent("foo").Get(1).IsZero())
	assert.False(t, NewEvent("foo").Get(0).IsZero())
	assert.True(t, NewEvent("foo", Int(1).As("bar")).Find("bleh").IsZero())
	assert.False(t, NewEvent("foo", Int(1).As("bar")).Find("bar").IsZero())
}
