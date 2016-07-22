package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListenerFunc(t *testing.T) {
	called := false
	fn := func(e Event, b []byte) {
		assert.Equal(t, "foo", e.Channel())
		assert.Equal(t, []byte{1, 2, 3}, b)
		called = true
	}

	listener := ListenerFunc(fn)
	listener.Handle(NewEvent().String("foo").ToEvent("foo", "foo"), []byte{1, 2, 3})
	assert.True(t, called)
}
