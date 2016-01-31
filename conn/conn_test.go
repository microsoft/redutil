package conn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidConnections(t *testing.T) {
	pool, _ := New(ConnectionParam{
		Address: "127.0.0.1:6379",
	}, 1)

	cnx := pool.Get()
	err := cnx.Err()

	assert.NotNil(t, cnx)
	assert.Nil(t, err)
}

func TestInvalidConnection(t *testing.T) {
	pool, _ := New(ConnectionParam{
		Address: "127.0.0.2:6379",
		Timeout: time.Nanosecond,
	}, 1)

	err := pool.Get().Err()

	assert.Equal(t, "dial tcp 127.0.0.2:6379: i/o timeout", err.Error())
}
