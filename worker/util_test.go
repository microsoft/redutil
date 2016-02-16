package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcatsErrors(t *testing.T) {
	ch1 := make(chan error)
	go func() {
		ch1 <- errors.New("a1")
		time.Sleep(10 * time.Millisecond)
		ch1 <- errors.New("a2")
		time.Sleep(10 * time.Millisecond)
		ch1 <- errors.New("a3")
		close(ch1)
	}()

	ch2 := make(chan error)
	go func() {
		time.Sleep(5 * time.Millisecond)
		ch1 <- errors.New("b1")
		close(ch2)
	}()

	tt := []struct {
		err    string
		closed bool
	}{
		{"a1", false},
		{"b1", false},
		{"a2", false},
		{"a3", false},
		{"", true},
	}
	out := concatErrs(ch1, ch2)

	for _, test := range tt {
		err, ok := <-out
		if test.closed {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, test.err, err.Error())
		}
	}
}
